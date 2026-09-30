package quorum

import (
	"sync"
)

// Replica is a single persistent-in-memory shard replica.
type Replica struct {
	mu sync.RWMutex

	id      string
	shardID string

	state       ReplicaState
	alive       bool
	currentTerm uint64
	votedFor    string
	commitIndex uint64

	// Entries are one-based; log[i-1].Index == i.
	log     []Entry
	data    map[string]*Value
	results map[string]committedResult
}

type committedResult struct {
	requestID      string
	key            string
	value          []byte
	compareVersion *uint64
	version        uint64
	commitIndex    uint64
	commitTerm     uint64
}

// NewReplica creates an empty follower replica.
func NewReplica(id, shardID string) *Replica {
	return &Replica{
		id:      id,
		shardID: shardID,
		state:   ReplicaStateFollower,
		alive:   true,
		data:    make(map[string]*Value),
		results: make(map[string]committedResult),
	}
}

func (r *Replica) ID() string      { return r.id }
func (r *Replica) ShardID() string { return r.shardID }

func (r *Replica) Status() ReplicaStatus {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.statusLocked()
}

func (r *Replica) statusLocked() ReplicaStatus {
	var version uint64
	for _, v := range r.data {
		if v.Version > version {
			version = v.Version
		}
	}
	return ReplicaStatus{
		ID:          r.id,
		ShardID:     r.shardID,
		State:       r.state,
		Alive:       r.alive,
		CurrentTerm: r.currentTerm,
		CommitIndex: r.commitIndex,
		LogLength:   uint64(len(r.log)),
		DataVersion: version,
	}
}

func (r *Replica) lastIndexLocked() uint64 {
	return uint64(len(r.log))
}

func (r *Replica) committedResultLocked(requestID string) (committedResult, bool) {
	result, ok := r.results[requestID]
	if !ok {
		return committedResult{}, false
	}
	result.value = cloneBytes(result.value)
	result.compareVersion = cloneUint64Ptr(result.compareVersion)
	return result, ok
}

func (r *Replica) keyValueLocked(key string) *Value {
	return cloneValue(r.data[key])
}

func (r *Replica) lastTermLocked() uint64 {
	if len(r.log) == 0 {
		return 0
	}
	return r.log[len(r.log)-1].Term
}

// logUpToDate implements Raft's log-up-to-date comparison.
func logUpToDate(candidateLastTerm, candidateLastIndex, voterLastTerm, voterLastIndex uint64) bool {
	if candidateLastTerm != voterLastTerm {
		return candidateLastTerm > voterLastTerm
	}
	return candidateLastIndex >= voterLastIndex
}

func (r *Replica) becomeFollowerLocked(term uint64) {
	if term > r.currentTerm {
		r.votedFor = ""
	}
	r.currentTerm = term
	r.state = ReplicaStateFollower
}

func (r *Replica) discardUncommittedLocked() {
	r.log = r.log[:r.commitIndex]
}

func (r *Replica) appendEntryLocked(entry Entry) {
	r.log = append(r.log, cloneEntry(entry))
}

func (r *Replica) applyCommittedLocked(upTo uint64) error {
	if upTo > r.lastIndexLocked() {
		return newError(KindInvalidArgument, "apply", r.shardID,
			"commit index exceeds local log length")
	}
	for r.commitIndex < upTo {
		next := r.commitIndex + 1
		entry := r.log[next-1]
		if entry.Admin {
			r.commitIndex = next
			continue
		}
		current := r.data[entry.Key]
		var currentVersion uint64
		if current != nil {
			currentVersion = current.Version
		}
		if entry.CompareVersion != nil && currentVersion != *entry.CompareVersion {
			return newError(KindVersionConflict, "apply", r.shardID,
				"committed compare-and-swap entry no longer matches local state")
		}
		version := currentVersion + 1
		value := &Value{
			Data:        cloneBytes(entry.Value),
			Version:     version,
			CommitIndex: entry.Index,
			CommitTerm:  entry.Term,
			RequestID:   entry.RequestID,
		}
		r.data[entry.Key] = value
		r.results[entry.RequestID] = committedResult{
			requestID:      entry.RequestID,
			key:            entry.Key,
			value:          cloneBytes(entry.Value),
			compareVersion: cloneUint64Ptr(entry.CompareVersion),
			version:        version,
			commitIndex:    entry.Index,
			commitTerm:     entry.Term,
		}
		r.commitIndex = next
	}
	return nil
}

func (r *Replica) appendEntriesLocked(term, prevLogIndex, prevLogTerm, leaderCommit uint64, entries []Entry) bool {
	if term < r.currentTerm {
		return false
	}
	r.currentTerm = term
	r.state = ReplicaStateFollower
	r.votedFor = ""

	if prevLogIndex > 0 {
		if uint64(len(r.log)) < prevLogIndex {
			return false
		}
		if r.log[prevLogIndex-1].Term != prevLogTerm {
			if prevLogIndex <= r.commitIndex {
				return false
			}
			r.log = append([]Entry(nil), r.log[:prevLogIndex-1]...)
		}
	}
	if len(entries) > 0 && uint64(len(r.log)) > prevLogIndex {
		conflictAt := -1
		limit := len(entries)
		if have := uint64(len(r.log)) - prevLogIndex; have < uint64(limit) {
			limit = int(have)
		}
		for i := 0; i < limit; i++ {
			logIndex := prevLogIndex + uint64(i)
			if !sameEntryCommand(r.log[logIndex], entries[i]) {
				conflictAt = i
				break
			}
		}
		if conflictAt >= 0 {
			// logIndex is zero-based; zero-based positions below commitIndex
			// correspond to one-based entries already committed.
			logIndex := prevLogIndex + uint64(conflictAt)
			if logIndex < r.commitIndex {
				return false
			}
			r.log = append([]Entry(nil), r.log[:logIndex]...)
		}
	}
	for i := uint64(len(r.log)); i-prevLogIndex < uint64(len(entries)); i++ {
		entry := entries[i-prevLogIndex]
		if entry.Index != i+1 {
			return false
		}
		r.log = append(r.log, cloneEntry(entry))
	}
	if leaderCommit > r.commitIndex {
		lastNew := prevLogIndex + uint64(len(entries))
		applyTo := leaderCommit
		if lastNew < applyTo {
			applyTo = lastNew
		}
		if err := r.applyCommittedLocked(applyTo); err != nil {
			return false
		}
	}
	return true
}

// overwriteCommittedLocked replaces divergent uncommitted suffixes and catches
// committed state up to a leader's committed prefix.
func (r *Replica) overwriteCommittedLocked(entries []Entry, leaderCommit uint64) error {
	if leaderCommit > uint64(len(entries)) {
		return newError(KindInvalidArgument, "catch-up", r.shardID,
			"leader commit index exceeds supplied log length")
	}

	// Any disagreement inside the local committed prefix would violate the
	// election/log invariant. Disagreement after that prefix is safe to cut.
	common := uint64(len(r.log))
	if limit := uint64(len(entries)); common > limit {
		common = limit
	}
	for i := uint64(0); i < common; i++ {
		if r.log[i].Index == entries[i].Index && sameEntryCommand(r.log[i], entries[i]) {
			continue
		}
		if i < r.commitIndex {
			return newError(KindVersionConflict, "catch-up", r.shardID,
				"leader log conflicts with an already committed local entry")
		}
		r.log = append([]Entry(nil), r.log[:i]...)
		break
	}

	// Cut local uncommitted entries only after the end of the supplied prefix.
	if uint64(len(r.log)) > uint64(len(entries)) {
		if uint64(len(entries)) < r.commitIndex {
			return newError(KindVersionConflict, "catch-up", r.shardID,
				"leader log is shorter than local committed log")
		}
		r.log = append([]Entry(nil), r.log[:len(entries)]...)
	}

	// Append missing entries. Do not use the old commitIndex to truncate here:
	// doing so would remove entries just caught from the current leader.
	for i := uint64(len(r.log)); i < uint64(len(entries)); i++ {
		r.log = append(r.log, cloneEntry(entries[i]))
	}
	if leaderCommit < r.commitIndex {
		return newError(KindVersionConflict, "catch-up", r.shardID,
			"leader commit index is behind local commit index")
	}
	return r.applyCommittedLocked(leaderCommit)
}
