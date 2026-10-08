package quorum

import (
	"context"
	"fmt"
	"log"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"
)

// ReplicaSet owns one shard's in-memory replicas, fault model and leader term.
type ReplicaSet struct {
	mu       sync.Mutex
	spec     ShardSpec
	replicas map[string]*Replica
	order    []string
	alive    map[string]bool
	blocked  map[string]bool
	leaderID string
	logger   Logger

	// Membership: voters are the replicas whose acknowledgement counts
	// toward the active quorum; learners catch the committed prefix but do
	// not vote. During a joint change both old and new majorities are
	// required. order always lists every known member.
	persister *persistence
	voters    []string
	learners  []string
	joint     bool
	oldVoters []string
	newVoters []string

	// Background supervisor lifecycle.
	tickInterval time.Duration
	elapsedTicks map[string]int
	running      bool
	stopCh       chan struct{}
	wg           sync.WaitGroup
}

// NewReplicaSet constructs a shard replica set and validates its configuration.
func NewReplicaSet(spec ShardSpec) (*ReplicaSet, error) {
	if strings.TrimSpace(spec.ID) == "" {
		return nil, newError(KindInvalidConfig, "new-replica-set", "", "shard id is empty")
	}
	if len(spec.ReplicaIDs) < 2 {
		return nil, newError(KindInvalidConfig, "new-replica-set", spec.ID, "at least two replicas are required")
	}
	if spec.Range.End != "" && spec.Range.Start >= spec.Range.End {
		return nil, newError(KindInvalidConfig, "new-replica-set", spec.ID, "half-open range start must be before end")
	}

	seen := make(map[string]bool, len(spec.ReplicaIDs))
	order := make([]string, 0, len(spec.ReplicaIDs))
	replicas := make(map[string]*Replica, len(spec.ReplicaIDs))
	for _, id := range spec.ReplicaIDs {
		if strings.TrimSpace(id) == "" {
			return nil, newError(KindInvalidConfig, "new-replica-set", spec.ID, "replica id is empty")
		}
		if seen[id] {
			return nil, newError(KindInvalidConfig, "new-replica-set", spec.ID, "duplicate replica id "+id)
		}
		seen[id] = true
		order = append(order, id)
		replicas[id] = NewReplica(id, spec.ID)
	}

	cloned := ShardSpec{
		ID:         spec.ID,
		Range:      spec.Range,
		ReplicaIDs: append([]string(nil), order...),
	}
	return &ReplicaSet{
		spec:         cloned,
		replicas:     replicas,
		order:        order,
		voters:       append([]string(nil), order...),
		alive:        maps.Clone(seen),
		blocked:      make(map[string]bool),
		elapsedTicks: make(map[string]int),
		tickInterval: DefaultTickInterval,
		logger:       log.Default(),
	}, nil
}

// SetLogger replaces the diagnostic logger.
func (s *ReplicaSet) SetLogger(logger Logger) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if logger != nil {
		s.logger = logger
	}
}

func (s *ReplicaSet) logf(format string, args ...any) {
	if s.logger != nil {
		s.logger.Printf(format, args...)
	}
}

// persistReplicaLocked flushes one replica's committed prefix, term and vote.
// The caller holds the set lock and the replica lock. When no data directory
// is configured it is a no-op, preserving the pure in-memory behavior.
func (s *ReplicaSet) persistReplicaLocked(replica *Replica) error {
	if s.persister == nil || !s.persister.enabled() {
		return nil
	}
	return s.persister.saveReplica(s.spec.ID, replica.snapshotLocked())
}

// persistReplicaSetLocked acquires the replica lock for the flush.
func (s *ReplicaSet) persistReplicaSetLocked(replica *Replica) error {
	replica.mu.Lock()
	defer replica.mu.Unlock()
	return s.persistReplicaLocked(replica)
}

func (s *ReplicaSet) Spec() ShardSpec {
	s.mu.Lock()
	defer s.mu.Unlock()
	spec := s.spec
	spec.ReplicaIDs = append([]string(nil), s.spec.ReplicaIDs...)
	return spec
}

func (s *ReplicaSet) LeaderID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.leaderID
}

func (s *ReplicaSet) Term() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.leaderID == "" {
		return 0
	}
	leader := s.replicas[s.leaderID]
	leader.mu.RLock()
	defer leader.mu.RUnlock()
	return leader.currentTerm
}

func (s *ReplicaSet) quorumLocked() int {
	return s.quorumForLocked(s.activeBallotModeLocked())
}

// jointQuorumsLocked reports the two distinct majorities required while a
// joint configuration entry is being committed.
func (s *ReplicaSet) jointQuorumsLocked() (oldQ, newQ int) {
	return majorityOf(len(s.oldVoters)), majorityOf(len(s.newVoters))
}

func partitionKey(a, b string) string {
	if a < b {
		return a + "\x00" + b
	}
	return b + "\x00" + a
}

func (s *ReplicaSet) blockedLocked(a, b string) bool {
	if a == b {
		return false
	}
	return s.blocked[partitionKey(a, b)]
}

func (s *ReplicaSet) reachableLocked(from, to string) bool {
	if from == to {
		return s.alive[from]
	}
	return s.alive[from] && s.alive[to] && !s.blockedLocked(from, to)
}

func (s *ReplicaSet) reachableFromLocked(from string) []string {
	out := make([]string, 0, len(s.order))
	for _, id := range s.order {
		if s.reachableLocked(from, id) {
			out = append(out, id)
		}
	}
	return out
}

func (s *ReplicaSet) requireReplicaLocked(id string) error {
	if _, ok := s.replicas[id]; !ok {
		return newError(KindNoReplica, "replica", s.spec.ID, "unknown replica "+id)
	}
	return nil
}

func (s *ReplicaSet) maxAliveTermLocked() uint64 {
	var maxTerm uint64
	for _, id := range s.order {
		if s.alive[id] && s.replicas[id].currentTerm > maxTerm {
			maxTerm = s.replicas[id].currentTerm
		}
	}
	return maxTerm
}

func (s *ReplicaSet) maxTermAmongLocked(ids []string) uint64 {
	var maxTerm uint64
	for _, id := range ids {
		if term := s.replicas[id].currentTerm; term > maxTerm {
			maxTerm = term
		}
	}
	return maxTerm
}

func (s *ReplicaSet) invalidateStaleLeaderLocked(reason string) bool {
	if s.leaderID == "" {
		return false
	}
	leader := s.replicas[s.leaderID]
	reachableVoters := s.reachableMembersLocked(s.leaderID, s.voters)
	reachableAll := s.reachableFromLocked(s.leaderID)
	newerTerm := s.maxTermAmongLocked(reachableAll)
	if s.alive[leader.id] && len(reachableVoters) >= s.quorumLocked() && newerTerm <= leader.currentTerm {
		return false
	}
	leader.mu.Lock()
	oldTerm := leader.currentTerm
	if newerTerm > leader.currentTerm {
		leader.becomeFollowerLocked(newerTerm)
	} else {
		leader.state = ReplicaStateFollower
	}
	persistErr := s.persistReplicaLocked(leader)
	leader.mu.Unlock()
	if persistErr != nil {
		s.logf("shard=%s failed to flush demotion of leader=%s term %d->%d: %v",
			s.spec.ID, leader.id, oldTerm, newerTerm, persistErr)
	}
	s.logf("shard=%s invalidate stale leader=%s reason=%s reachableVoters=%d quorum=%d newerTerm=%d oldTerm=%d",
		s.spec.ID, leader.id, reason, len(reachableVoters), s.quorumLocked(), newerTerm, leader.currentTerm)
	s.leaderID = ""
	return true
}

func (s *ReplicaSet) resetForFollowerLocked(leader, follower *Replica, reason string) {
	follower.mu.Lock()
	defer follower.mu.Unlock()
	oldTerm := follower.currentTerm
	if follower.currentTerm != leader.currentTerm || follower.state != ReplicaStateFollower {
		s.logf("shard=%s reset replica=%s to follower term=%d reason=%s oldTerm=%d oldState=%d",
			s.spec.ID, follower.id, leader.currentTerm, reason, follower.currentTerm, follower.state)
	}
	follower.becomeFollowerLocked(leader.currentTerm)
	if oldTerm != follower.currentTerm {
		if err := s.persistReplicaLocked(follower); err != nil {
			s.logf("shard=%s failed to flush term bump on replica=%s term %d->%d: %v",
				s.spec.ID, follower.id, oldTerm, follower.currentTerm, err)
		}
	}
}

// syncCommittedPrefixLocked makes a reachable follower's committed log and key
// state equal to the leader's committed prefix. It never copies uncommitted
// entries, so an isolated replica cannot publish a half-replicated write.
func (s *ReplicaSet) syncCommittedPrefixLocked(leader, follower *Replica) error {
	if leader == follower {
		return nil
	}
	if !s.reachableLocked(leader.id, follower.id) {
		return newError(KindReplicaUnavailable, "sync", s.spec.ID,
			"replica "+follower.id+" is not reachable from leader "+leader.id)
	}

	leader.mu.RLock()
	leaderTerm := leader.currentTerm
	leaderCommit := leader.commitIndex
	committed := make([]Entry, leaderCommit)
	copy(committed, leader.log[:leaderCommit])
	leader.mu.RUnlock()

	follower.mu.RLock()
	followerTerm := follower.currentTerm
	follower.mu.RUnlock()
	if followerTerm > leaderTerm {
		s.logf("shard=%s refuse to demote follower=%s term=%d behind leader=%s term=%d; a new election is required",
			s.spec.ID, follower.id, followerTerm, leader.id, leaderTerm)
		return newError(KindNotLeader, "sync", s.spec.ID,
			"follower has a newer term than the presumed leader")
	}

	// Crash recovery can expose an old candidate with a term gained from a
	// failed minority campaign. Such a term committed no writes; once the
	// current leader proves a committed prefix, the recovered node rejoins it.
	s.resetForFollowerLocked(leader, follower, "sync-committed-prefix")
	follower.mu.Lock()
	defer follower.mu.Unlock()
	if err := follower.overwriteCommittedLocked(committed, leaderCommit); err != nil {
		s.logf("shard=%s catch-up replica=%s from leader=%s failed invariant: %v",
			s.spec.ID, follower.id, leader.id, err)
		return err
	}
	if err := s.persistReplicaLocked(follower); err != nil {
		return err
	}
	s.logf("shard=%s caught up replica=%s to leader=%s commitIndex=%d term=%d",
		s.spec.ID, follower.id, leader.id, leaderCommit, leaderTerm)
	return nil
}

func (s *ReplicaSet) appendAndCommitLocked(leader *Replica, entry Entry) (commitPolicy, error) {
	return s.appendAndCommitModeLocked(leader, entry, s.activeBallotModeLocked(), nil)
}

// ballot tracks acknowledgements separately per membership side during a
// joint commit, while keeping a single monotonic counter for the common path.
type commitPolicy struct {
	mode    ballotMode
	oldIDs  []string
	newIDs  []string
	oldQ    int
	newQ    int
	oldAcks int
	newAcks int
}

func (s *ReplicaSet) newCommitPolicyLocked(mode ballotMode) commitPolicy {
	p := commitPolicy{mode: mode}
	switch mode {
	case ballotJoint:
		p.oldIDs = s.oldVoters
		p.newIDs = s.newVoters
		p.oldQ, p.newQ = s.jointQuorumsLocked()
	default:
		p.oldIDs = s.voters
		p.oldQ = s.quorumForLocked(ballotNormal)
	}
	return p
}

func (p *commitPolicy) members() []string {
	if p.mode == ballotJoint {
		// Every member of either side must receive the joint entry, but only
		// voter acks count. Union keeps the set deduplicated.
		seen := make(map[string]bool, len(p.oldIDs)+len(p.newIDs))
		out := make([]string, 0, len(p.oldIDs)+len(p.newIDs))
		for _, id := range append(append([]string(nil), p.oldIDs...), p.newIDs...) {
			if !seen[id] {
				seen[id] = true
				out = append(out, id)
			}
		}
		return out
	}
	return p.oldIDs
}

func (p *commitPolicy) countAck(id string) {
	if p.mode == ballotJoint {
		if containsID(p.oldIDs, id) {
			p.oldAcks++
		}
		if containsID(p.newIDs, id) {
			p.newAcks++
		}
		return
	}
	if containsID(p.oldIDs, id) {
		p.oldAcks++
	}
}

func (p *commitPolicy) satisfied() bool {
	if p.mode == ballotJoint {
		return p.oldAcks >= p.oldQ && p.newAcks >= p.newQ
	}
	return p.oldAcks >= p.oldQ
}

func (p *commitPolicy) describe() string {
	if p.mode == ballotJoint {
		return fmt.Sprintf("joint oldAcks=%d/%d newAcks=%d/%d", p.oldAcks, p.oldQ, p.newAcks, p.newQ)
	}
	return fmt.Sprintf("acks=%d/%d", p.oldAcks, p.oldQ)
}

// appendAndCommitModeLocked replicates one entry and waits for the quorum
// dictated by mode. onCommitted runs after the leader applies the entry but
// before followers are notified, which is exactly the instant a membership
// change becomes effective.
func (s *ReplicaSet) appendAndCommitModeLocked(leader *Replica, entry Entry, mode ballotMode,
	onCommitted func() error) (commitPolicy, error) {
	policy := s.newCommitPolicyLocked(mode)
	targets := policy.members()

	// Count reachable acks per membership side, including the leader's own
	// vote, before appending anything. This is the same accounting used after
	// replication, so a missing majority is rejected up front.
	pre := commitPolicy{mode: policy.mode, oldIDs: policy.oldIDs, newIDs: policy.newIDs,
		oldQ: policy.oldQ, newQ: policy.newQ}
	reachableTargets := make([]string, 0, len(targets))
	for _, id := range targets {
		if s.reachableLocked(leader.id, id) {
			reachableTargets = append(reachableTargets, id)
			pre.countAck(id)
		}
	}
	// Pre-check: even before appending, an unreachable old or new majority
	// makes a joint commit impossible; refuse instead of half-committing.
	if mode == ballotJoint {
		if pre.oldAcks < pre.oldQ || pre.newAcks < pre.newQ {
			return policy, newError(KindQuorumUnavailable, "append", s.spec.ID,
				fmt.Sprintf("joint quorum unreachable before append: old=%d/%d new=%d/%d",
					pre.oldAcks, pre.oldQ, pre.newAcks, pre.newQ))
		}
	} else if pre.oldAcks < pre.oldQ {
		return policy, newError(KindQuorumUnavailable, "append", s.spec.ID,
			fmt.Sprintf("reachable voters below quorum before appending: %s", pre.describe()))
	}

	leader.mu.Lock()
	leader.discardUncommittedLocked()
	leader.appendEntryLocked(entry)
	term := leader.currentTerm
	oldCommit := entry.Index - 1
	var oldCommitTerm uint64
	if oldCommit > 0 {
		oldCommitTerm = leader.log[oldCommit-1].Term
	}
	leader.mu.Unlock()

	entriesToSend := []Entry{cloneEntry(entry)}
	policy.countAck(leader.id)

	for _, id := range reachableTargets {
		if id == leader.id {
			continue
		}
		follower := s.replicas[id]
		if err := s.syncCommittedPrefixLocked(leader, follower); err != nil {
			s.logf("shard=%s entry index=%d failed to synchronize member=%s: %v",
				s.spec.ID, entry.Index, id, err)
			continue
		}
		follower.mu.Lock()
		accepted := follower.appendEntriesLocked(term, oldCommit,
			oldCommitTerm, 0, entriesToSend)
		if !accepted {
			follower.mu.Unlock()
			s.logf("shard=%s member=%s rejected append index=%d after committed-prefix synchronization",
				s.spec.ID, id, entry.Index)
			continue
		}
		follower.mu.Unlock()
		policy.countAck(id)
	}

	if !policy.satisfied() {
		s.logf("shard=%s entry index=%d rejected: %s reachable=%v",
			s.spec.ID, entry.Index, policy.describe(), reachableTargets)
		return policy, newError(KindQuorumUnavailable, "append", s.spec.ID,
			"insufficient acknowledgements: "+policy.describe())
	}

	leader.mu.Lock()
	if err := leader.applyCommittedLocked(entry.Index); err != nil {
		leader.mu.Unlock()
		return policy, err
	}
	if err := s.persistReplicaLocked(leader); err != nil {
		leader.mu.Unlock()
		return policy, err
	}
	leader.mu.Unlock()

	if onCommitted != nil {
		if err := onCommitted(); err != nil {
			return policy, err
		}
	}

	for _, id := range reachableTargets {
		if id == leader.id {
			continue
		}
		follower := s.replicas[id]
		follower.mu.Lock()
		if !follower.appendEntriesLocked(term, entry.Index, entry.Term, entry.Index, nil) {
			s.logf("shard=%s post-commit member=%s cannot advance to index=%d; committed result remains durable on quorum",
				s.spec.ID, id, entry.Index)
		} else if err := s.persistReplicaLocked(follower); err != nil {
			follower.mu.Unlock()
			return policy, err
		}
		follower.mu.Unlock()
	}
	s.logf("shard=%s committed entry index=%d term=%d %s replicas=%v",
		s.spec.ID, entry.Index, entry.Term, policy.describe(), reachableTargets)
	return policy, nil
}

func (s *ReplicaSet) tryCandidateLocked(candidateID string, term uint64) bool {
	candidate := s.replicas[candidateID]
	if !s.isVoterLocked(candidateID) {
		s.logf("shard=%s candidate=%s cannot campaign: non-voter (learner or removed member)",
			s.spec.ID, candidateID)
		return false
	}
	reachable := s.reachableMembersLocked(candidateID, s.voters)
	if len(reachable) < s.quorumLocked() {
		s.logf("shard=%s candidate=%s cannot campaign: reachableVoters=%d quorum=%d peers=%v",
			s.spec.ID, candidateID, len(reachable), s.quorumLocked(), reachable)
		return false
	}

	candidate.mu.Lock()
	candidate.currentTerm = term
	candidate.votedFor = candidateID
	candidate.state = ReplicaStateCandidate
	candidate.discardUncommittedLocked()
	lastIndex := candidate.lastIndexLocked()
	lastTerm := candidate.lastTermLocked()
	if err := s.persistReplicaLocked(candidate); err != nil {
		candidate.mu.Unlock()
		s.logf("shard=%s candidate=%s term=%d cannot make term durable: %v",
			s.spec.ID, candidateID, term, err)
		return false
	}
	candidate.mu.Unlock()

	votes := 1
	for _, voterID := range reachable {
		if voterID == candidateID {
			continue
		}
		voter := s.replicas[voterID]
		voter.mu.Lock()
		if voter.currentTerm > term {
			s.logf("shard=%s candidate=%s term=%d rejected vote from=%s: voter newer term=%d",
				s.spec.ID, candidateID, term, voterID, voter.currentTerm)
			voter.mu.Unlock()
			continue
		}
		if voter.currentTerm < term {
			voter.becomeFollowerLocked(term)
			if err := s.persistReplicaLocked(voter); err != nil {
				voter.mu.Unlock()
				s.logf("shard=%s candidate=%s term=%d could not flush term on voter=%s: %v",
					s.spec.ID, candidateID, term, voterID, err)
				continue
			}
		}
		if voter.votedFor != "" && voter.votedFor != candidateID {
			s.logf("shard=%s candidate=%s rejected vote from=%s: already voted for=%s term=%d",
				s.spec.ID, candidateID, voterID, voter.votedFor, term)
			voter.mu.Unlock()
			continue
		}
		if !logUpToDate(lastTerm, lastIndex, voter.lastTermLocked(), voter.lastIndexLocked()) {
			s.logf("shard=%s candidate=%s rejected vote from=%s: candidate=(term=%d,index=%d) voter=(term=%d,index=%d)",
				s.spec.ID, candidateID, voterID, lastTerm, lastIndex,
				voter.lastTermLocked(), voter.lastIndexLocked())
			voter.mu.Unlock()
			continue
		}
		voter.votedFor = candidateID
		voter.state = ReplicaStateFollower
		if err := s.persistReplicaLocked(voter); err != nil {
			voter.mu.Unlock()
			s.logf("shard=%s candidate=%s vote from=%s term=%d could not be made durable: %v",
				s.spec.ID, candidateID, voterID, term, err)
			continue
		}
		voter.mu.Unlock()
		votes++
		s.logf("shard=%s candidate=%s granted vote from=%s term=%d votes=%d/%d",
			s.spec.ID, candidateID, voterID, term, votes, s.quorumLocked())
	}

	if votes < s.quorumLocked() {
		s.logf("shard=%s candidate=%s election failed: votes=%d quorum=%d reachable=%v",
			s.spec.ID, candidateID, votes, s.quorumLocked(), reachable)
		return false
	}

	candidate.mu.Lock()
	var knownCommit uint64
	for _, voterID := range reachable {
		if voterID == candidateID {
			continue
		}
		voter := s.replicas[voterID]
		voter.mu.RLock()
		if voter.commitIndex > knownCommit && voter.commitIndex <= lastIndex {
			knownCommit = voter.commitIndex
		}
		voter.mu.RUnlock()
	}
	if knownCommit > candidate.commitIndex {
		s.logf("shard=%s candidate=%s advances lagging local commit=%d to proven prefix=%d before taking office",
			s.spec.ID, candidateID, candidate.commitIndex, knownCommit)
		if err := candidate.applyCommittedLocked(knownCommit); err != nil {
			candidate.mu.Unlock()
			s.logf("shard=%s candidate=%s cannot take office: %v", s.spec.ID, candidateID, err)
			return false
		}
	}
	candidate.discardUncommittedLocked()
	candidate.state = ReplicaStateLeader
	candidate.mu.Unlock()

	s.leaderID = candidateID
	s.logf("shard=%s elected leader=%s term=%d votes=%d quorum=%d reachable=%v commitIndex=%d",
		s.spec.ID, candidateID, term, votes, s.quorumLocked(), reachable, candidate.commitIndex)

	for _, followerID := range reachable {
		if followerID == candidateID {
			continue
		}
		if err := s.syncCommittedPrefixLocked(candidate, s.replicas[followerID]); err != nil {
			s.leaderID = ""
			s.logf("shard=%s election of=%s rolled back due catch-up failure: %v",
				s.spec.ID, candidateID, err)
			return false
		}
	}

	candidate.mu.RLock()
	noop := Entry{
		Index: candidate.commitIndex + 1,
		Term:  term,
		Admin: true,
	}
	candidate.mu.RUnlock()
	acks, err := s.appendAndCommitLocked(candidate, noop)
	if err != nil {
		s.leaderID = ""
		s.logf("shard=%s leader=%s failed to commit term=%d no-op after %s: %v",
			s.spec.ID, candidateID, term, acks.describe(), err)
		return false
	}
	// If a previous leader crashed between the committed joint entry and the
	// new-config entry, the new leader takes over while the shard is still in
	// the (safe, stricter) joint state. Finish the pending change now: this
	// only succeeds when the new-config majority is reachable.
	if s.joint {
		if err := s.finalizeJointAfterElectionLocked(candidate); err != nil {
			s.logf("shard=%s leader=%s keeps serving in joint state; pending reconfiguration not yet completable: %v",
				s.spec.ID, candidateID, err)
		}
	}
	return true
}

func (s *ReplicaSet) electLeaderLocked(preferred string) (string, error) {
	if preferred != "" {
		if err := s.requireReplicaLocked(preferred); err != nil {
			return "", err
		}
		if !s.alive[preferred] {
			return "", newError(KindReplicaUnavailable, "elect", s.spec.ID,
				"preferred replica "+preferred+" is crashed")
		}
	}

	if s.leaderID != "" {
		leader := s.replicas[s.leaderID]
		reachableVoters := s.reachableMembersLocked(s.leaderID, s.voters)
		newerTerm := s.maxTermAmongLocked(s.reachableFromLocked(s.leaderID))
		if s.alive[leader.id] && len(reachableVoters) >= s.quorumLocked() && newerTerm <= leader.currentTerm {
			s.logf("shard=%s keep leader=%s: reachableVoters=%d quorum=%d term=%d maxTerm=%d",
				s.spec.ID, leader.id, len(reachableVoters), s.quorumLocked(), leader.currentTerm, newerTerm)
			return leader.id, nil
		}
		s.invalidateStaleLeaderLocked("explicit-election")
	}

	candidates := make([]string, 0, len(s.voters))
	for _, id := range s.voters {
		if s.alive[id] {
			candidates = append(candidates, id)
		}
	}
	slices.SortFunc(candidates, func(a, b string) int {
		if a == preferred && b != preferred {
			return -1
		}
		if b == preferred && a != preferred {
			return 1
		}
		ra := s.replicas[a]
		rb := s.replicas[b]
		ra.mu.RLock()
		rb.mu.RLock()
		ta, ia := ra.lastTermLocked(), ra.lastIndexLocked()
		tb, ib := rb.lastTermLocked(), rb.lastIndexLocked()
		ra.mu.RUnlock()
		rb.mu.RUnlock()
		if ta != tb {
			if ta > tb {
				return -1
			}
			return 1
		}
		if ia != ib {
			if ia > ib {
				return -1
			}
			return 1
		}
		if a < b {
			return -1
		}
		return 1
	})

	for _, candidateID := range candidates {
		term := s.maxAliveTermLocked() + 1
		s.logf("shard=%s starting election attempt candidate=%s proposedTerm=%d preferred=%s",
			s.spec.ID, candidateID, term, preferred)
		if s.tryCandidateLocked(candidateID, term) {
			return candidateID, nil
		}
	}
	return "", newError(KindQuorumUnavailable, "elect", s.spec.ID,
		"no reachable candidate could obtain a majority")
}

// ElectLeader explicitly runs a fault-aware election. An empty preferred ID
// chooses the reachable replica with the most recent log.
func (s *ReplicaSet) ElectLeader(preferred string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.electLeaderLocked(preferred)
}

func (s *ReplicaSet) CatchUp() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	aliveVoters := len(s.aliveVotersLocked())
	if aliveVoters < s.quorumLocked() {
		return newError(KindQuorumUnavailable, "catch-up", s.spec.ID,
			"alive voting replicas below majority")
	}

	if s.leaderID != "" {
		s.invalidateStaleLeaderLocked("catch-up")
	}
	if s.leaderID == "" || !s.alive[s.leaderID] ||
		len(s.reachableMembersLocked(s.leaderID, s.voters)) < s.quorumLocked() {
		if _, err := s.electLeaderLocked(""); err != nil {
			return err
		}
	}
	leader := s.replicas[s.leaderID]
	for _, id := range s.order {
		if id == leader.id || !s.alive[id] {
			continue
		}
		if err := s.syncCommittedPrefixLocked(leader, s.replicas[id]); err != nil {
			return err
		}
	}
	return nil
}

func (s *ReplicaSet) ensureLeaderForWriteLocked(op string) (*Replica, error) {
	if len(s.voters) < s.quorumLocked() {
		return nil, newError(KindQuorumUnavailable, op, s.spec.ID,
			"configured voter count cannot form a majority")
	}
	aliveVoters := len(s.aliveVotersLocked())
	if aliveVoters < s.quorumLocked() {
		s.logf("shard=%s %s rejected: aliveVoters=%d configuredVoters=%d quorum=%d",
			s.spec.ID, op, aliveVoters, len(s.voters), s.quorumLocked())
		return nil, newError(KindQuorumUnavailable, op, s.spec.ID,
			"alive voting replicas below majority; write is not degraded")
	}

	if s.leaderID != "" {
		if !s.alive[s.leaderID] {
			s.logf("shard=%s %s triggers election: leader=%s is crashed",
				s.spec.ID, op, s.leaderID)
			s.leaderID = ""
		} else {
			s.invalidateStaleLeaderLocked(op)
		}
	}
	if s.leaderID == "" {
		if _, err := s.electLeaderLocked(""); err != nil {
			s.logf("shard=%s %s rejected: automatic election failed: %v",
				s.spec.ID, op, err)
			return nil, err
		}
	}
	leader := s.replicas[s.leaderID]
	reachable := s.reachableMembersLocked(leader.id, s.voters)
	if len(reachable) < s.quorumLocked() {
		s.logf("shard=%s %s rejected after election: leader=%s reachableVoters=%d quorum=%d",
			s.spec.ID, op, leader.id, len(reachable), s.quorumLocked())
		return nil, newError(KindQuorumUnavailable, op, s.spec.ID,
			"leader cannot currently contact a voting majority")
	}
	return leader, nil
}

func sameWriteRequestLocked(result committedResult, req WriteRequest) bool {
	if result.key != req.Key || string(result.value) != string(req.Value) {
		return false
	}
	if result.compareVersion == nil || req.CompareVersion == nil {
		return result.compareVersion == nil && req.CompareVersion == nil
	}
	return *result.compareVersion == *req.CompareVersion
}

func (s *ReplicaSet) Write(ctx context.Context, req WriteRequest) (*WriteResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if req.ShardID != "" && req.ShardID != s.spec.ID {
		return nil, newError(KindKeyOutOfRange, "write", s.spec.ID,
			"request addressed shard "+req.ShardID)
	}
	if req.Key == "" {
		return nil, newError(KindInvalidArgument, "write", s.spec.ID, "key is empty")
	}
	if !s.spec.Range.Contains(req.Key) {
		return nil, newError(KindKeyOutOfRange, "write", s.spec.ID,
			"key "+req.Key+" is outside range ["+s.spec.Range.Start+", "+s.spec.Range.End+")")
	}
	if req.RequestID == "" {
		return nil, newError(KindMissingRequestID, "write", s.spec.ID,
			"idempotent writes must carry a request ID")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	leader, err := s.ensureLeaderForWriteLocked("write")
	if err != nil {
		return nil, err
	}
	leader.mu.Lock()
	if previous, ok := leader.committedResultLocked(req.RequestID); ok {
		duplicate := sameWriteRequestLocked(previous, req)
		leader.mu.Unlock()
		if !duplicate {
			s.logf("shard=%s request=%s rejected: same ID previously committed key=%s, request key=%s",
				s.spec.ID, req.RequestID, previous.key, req.Key)
			return nil, newError(KindDuplicateConflict, "write", s.spec.ID,
				"request ID "+req.RequestID+" was committed with different parameters")
		}
		s.logf("shard=%s request=%s served as committed duplicate version=%d index=%d",
			s.spec.ID, req.RequestID, previous.version, previous.commitIndex)
		return &WriteResponse{
			ShardID:     s.spec.ID,
			Key:         previous.key,
			Version:     previous.version,
			CommitIndex: previous.commitIndex,
			CommitTerm:  previous.commitTerm,
			LeaderID:    leader.id,
			RequestID:   req.RequestID,
			Duplicate:   true,
		}, nil
	}
	current := leader.keyValueLocked(req.Key)
	var currentVersion uint64
	if current != nil {
		currentVersion = current.Version
	}
	if req.CompareVersion != nil && currentVersion != *req.CompareVersion {
		leader.mu.Unlock()
		s.logf("shard=%s CAS key=%s request=%s rejected: expectedVersion=%d currentVersion=%d",
			s.spec.ID, req.Key, req.RequestID, *req.CompareVersion, currentVersion)
		return nil, newError(KindVersionConflict, "write", s.spec.ID,
			"expected version does not match current version")
	}
	nextIndex := leader.lastIndexLocked() + 1
	term := leader.currentTerm
	entry := Entry{
		Index:          nextIndex,
		Term:           term,
		RequestID:      req.RequestID,
		Key:            req.Key,
		Value:          cloneBytes(req.Value),
		CompareVersion: cloneUint64Ptr(req.CompareVersion),
	}
	leader.mu.Unlock()

	acks, err := s.appendAndCommitLocked(leader, entry)
	if err != nil {
		s.logf("shard=%s write key=%s request=%s rejected after %s: %v",
			s.spec.ID, req.Key, req.RequestID, acks.describe(), err)
		return nil, err
	}

	leader.mu.RLock()
	committed, ok := leader.committedResultLocked(req.RequestID)
	leader.mu.RUnlock()
	if !ok {
		return nil, newError(KindQuorumUnavailable, "write", s.spec.ID,
			"quorum acknowledged but leader cannot find committed result")
	}
	return &WriteResponse{
		ShardID:     s.spec.ID,
		Key:         committed.key,
		Version:     committed.version,
		CommitIndex: committed.commitIndex,
		CommitTerm:  committed.commitTerm,
		LeaderID:    leader.id,
		RequestID:   req.RequestID,
		Duplicate:   false,
	}, nil
}

func (s *ReplicaSet) prepareStrongReadLocked(op string) (*Replica, uint64, error) {
	if len(s.aliveVotersLocked()) < s.quorumLocked() {
		return nil, 0, newError(KindQuorumUnavailable, op, s.spec.ID,
			"alive voting replicas below majority for a strong read")
	}
	if s.leaderID == "" || !s.alive[s.leaderID] {
		if _, err := s.electLeaderLocked(""); err != nil {
			return nil, 0, err
		}
	}
	leader := s.replicas[s.leaderID]
	reachable := s.reachableMembersLocked(leader.id, s.voters)
	if len(reachable) < s.quorumLocked() {
		return nil, 0, newError(KindStrongReadUnsafe, op, s.spec.ID,
			"leader cannot contact a voting read quorum")
	}
	maxTerm := s.maxTermAmongLocked(s.reachableFromLocked(leader.id))
	if maxTerm > leader.currentTerm {
		s.invalidateStaleLeaderLocked(op)
		return nil, 0, newError(KindNotLeader, op, s.spec.ID,
			"a reachable replica has a newer term; leader confirmation failed")
	}
	leader.mu.RLock()
	commit := leader.commitIndex
	leader.mu.RUnlock()
	s.logf("shard=%s strong read confirmed leader=%s term=%d readQuorum=%d/%d commitIndex=%d",
		s.spec.ID, leader.id, leader.currentTerm, len(reachable), s.quorumLocked(), commit)
	return leader, commit, nil
}

func (s *ReplicaSet) maxCommitIndexLocked() uint64 {
	var maxCommit uint64
	for _, id := range s.order {
		if s.alive[id] {
			r := s.replicas[id]
			r.mu.RLock()
			if r.commitIndex > maxCommit {
				maxCommit = r.commitIndex
			}
			r.mu.RUnlock()
		}
	}
	return maxCommit
}

func (s *ReplicaSet) Read(ctx context.Context, req ReadRequest) (*ReadResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if req.ShardID != "" && req.ShardID != s.spec.ID {
		return nil, newError(KindKeyOutOfRange, "read", s.spec.ID,
			"request addressed shard "+req.ShardID)
	}
	if req.Key == "" {
		return nil, newError(KindInvalidArgument, "read", s.spec.ID, "key is empty")
	}
	if !s.spec.Range.Contains(req.Key) {
		return nil, newError(KindKeyOutOfRange, "read", s.spec.ID,
			"key "+req.Key+" is outside range ["+s.spec.Range.Start+", "+s.spec.Range.End+")")
	}
	if req.Mode != ReadModeStrong && req.Mode != ReadModeEventual {
		return nil, newError(KindInvalidReadMode, "read", s.spec.ID, "read mode must be strong or eventual")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if req.Mode == ReadModeStrong {
		if req.ReplicaID != "" {
			return nil, newError(KindInvalidReadMode, "read", s.spec.ID,
				"strong reads are served through the confirmed leader; clear ReplicaID")
		}
		leader, commit, err := s.prepareStrongReadLocked("strong-read")
		if err != nil {
			return nil, err
		}
		if req.MinIndex > commit {
			return nil, newError(KindStrongReadUnsafe, "read", s.spec.ID,
				"confirmed commit index is behind the requested minimum index")
		}
		leader.mu.RLock()
		value := leader.keyValueLocked(req.Key)
		replicaID := leader.id
		var commitTerm uint64
		if commit > 0 {
			commitTerm = leader.log[commit-1].Term
		}
		leader.mu.RUnlock()
		return &ReadResponse{
			ShardID:     s.spec.ID,
			Key:         req.Key,
			Value:       value,
			ReplicaID:   replicaID,
			CommitIndex: commit,
			CommitTerm:  commitTerm,
			Stale:       false,
		}, nil
	}

	if req.MinIndex > 0 {
		return nil, newError(KindInvalidReadMode, "read", s.spec.ID,
			"MinIndex requires a strong read; eventual reads cannot promise it")
	}
	replicaID := req.ReplicaID
	if replicaID == "" {
		if s.leaderID != "" && s.alive[s.leaderID] {
			replicaID = s.leaderID
		} else {
			for _, id := range s.order {
				if s.alive[id] {
					replicaID = id
					break
				}
			}
		}
	}
	if replicaID == "" {
		return nil, newError(KindNoReplica, "eventual-read", s.spec.ID, "no live replica is available")
	}
	if err := s.requireReplicaLocked(replicaID); err != nil {
		return nil, err
	}
	if !s.alive[replicaID] {
		return nil, newError(KindReplicaUnavailable, "eventual-read", s.spec.ID,
			"replica "+replicaID+" is crashed")
	}
	replica := s.replicas[replicaID]
	replica.mu.RLock()
	value := replica.keyValueLocked(req.Key)
	commit := replica.commitIndex
	var commitTerm uint64
	if commit > 0 {
		commitTerm = replica.log[commit-1].Term
	}
	replica.mu.RUnlock()
	maxCommit := s.maxCommitIndexLocked()
	reachableCount := len(s.reachableFromLocked(replicaID))
	quorumUnavailable := reachableCount < s.quorumLocked()
	stale := commit < maxCommit || quorumUnavailable
	s.logf("shard=%s eventual read replica=%s commitIndex=%d maxAliveCommit=%d reachable=%d/%d stale=%t",
		s.spec.ID, replicaID, commit, maxCommit, reachableCount, s.quorumLocked(), stale)
	return &ReadResponse{
		ShardID:     s.spec.ID,
		Key:         req.Key,
		Value:       value,
		ReplicaID:   replicaID,
		CommitIndex: commit,
		CommitTerm:  commitTerm,
		Stale:       stale,
	}, nil
}

func (s *ReplicaSet) CrashReplica(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.requireReplicaLocked(id); err != nil {
		return err
	}
	if !s.alive[id] {
		s.logf("shard=%s crash replica=%s rejected: replica is already crashed", s.spec.ID, id)
		return newError(KindReplicaUnavailable, "crash", s.spec.ID, "replica "+id+" is already crashed")
	}
	s.alive[id] = false
	if s.leaderID == id {
		s.logf("shard=%s leader=%s crashed; leader record retained until a write or explicit election proves a successor",
			s.spec.ID, id)
	} else {
		s.logf("shard=%s follower=%s crashed; stored log and key state retained", s.spec.ID, id)
	}
	return nil
}

func (s *ReplicaSet) RecoverReplica(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.requireReplicaLocked(id); err != nil {
		return err
	}
	if s.alive[id] {
		s.logf("shard=%s recover replica=%s rejected: replica is already alive", s.spec.ID, id)
		return newError(KindReplicaUnavailable, "recover", s.spec.ID, "replica "+id+" is already alive")
	}
	replica := s.replicas[id]
	replica.mu.RLock()
	commitIndex := replica.commitIndex
	logLength := uint64(len(replica.log))
	replica.mu.RUnlock()
	s.alive[id] = true
	s.logf("shard=%s replica=%s recovered with commit=%d log=%d; it remains read-only until catch-up proves the current prefix",
		s.spec.ID, id, commitIndex, logLength)
	return nil
}

func (s *ReplicaSet) AddPartition(a, b string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.requireReplicaLocked(a); err != nil {
		return err
	}
	if err := s.requireReplicaLocked(b); err != nil {
		return err
	}
	if a == b {
		return newError(KindInvalidArgument, "add-partition", s.spec.ID, "partition endpoints must differ")
	}
	s.blocked[partitionKey(a, b)] = true
	s.logf("shard=%s injected undirected partition between=%s,%s", s.spec.ID, a, b)
	return nil
}

func (s *ReplicaSet) RemovePartition(a, b string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.requireReplicaLocked(a); err != nil {
		return err
	}
	if err := s.requireReplicaLocked(b); err != nil {
		return err
	}
	delete(s.blocked, partitionKey(a, b))
	s.logf("shard=%s removed partition between=%s,%s", s.spec.ID, a, b)
	return nil
}

func (s *ReplicaSet) HealPartitions() {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := len(s.blocked)
	clear(s.blocked)
	s.logf("shard=%s healed %d injected partition edges", s.spec.ID, n)
}

func (s *ReplicaSet) Status() map[string]ReplicaStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]ReplicaStatus, len(s.order))
	for _, id := range s.order {
		status := s.replicas[id].Status()
		status.Alive = s.alive[id]
		status.Voter = s.isVoterLocked(id)
		status.Learner = containsID(s.learners, id)
		out[id] = status
	}
	return out
}
