package quorum

// finalizeJointAfterElectionLocked completes a membership change whose joint
// entry was committed by a previous leader that crashed before appending the
// new-config entry. It commits a fresh new-config entry under the new quorum.
// Failure leaves the safe joint state intact; the next leader or a retry will
// finish it.
func (s *ReplicaSet) finalizeJointAfterElectionLocked(leader *Replica) error {
	oldVoters := append([]string(nil), s.oldVoters...)
	newVoters := append([]string(nil), s.newVoters...)
	leader.mu.RLock()
	nextIndex := leader.lastIndexLocked() + 1
	term := leader.currentTerm
	leader.mu.RUnlock()

	// Ensure the new members exist locally and carry the committed prefix.
	for _, id := range newVoters {
		if _, exists := s.replicas[id]; !exists {
			s.replicas[id] = NewReplica(id, s.spec.ID)
			s.alive[id] = true
			s.order = append(s.order, id)
			s.elapsedTicks[id] = electionOffsetTicks(id)
		}
		if !containsID(s.learners, id) && !containsID(s.voters, id) {
			s.learners = append(s.learners, id)
		}
		if err := s.syncCommittedPrefixLocked(leader, s.replicas[id]); err != nil {
			return err
		}
	}

	newEntry := Entry{
		Index:        nextIndex,
		Term:         term,
		Admin:        true,
		RequestID:    "config-new-resume",
		ConfigChange: string(configKindNew),
		NewVoters:    append([]string(nil), newVoters...),
	}
	activateConfig := func() error {
		s.joint = false
		s.voters = newVoters
		s.learners = nil
		s.oldVoters = nil
		s.newVoters = nil
		s.spec.ReplicaIDs = append([]string(nil), newVoters...)
		s.logf("shard=%s resumed pending reconfiguration at index=%d; quorum now %d over voters=%v",
			s.spec.ID, nextIndex, s.quorumLocked(), newVoters)
		return s.saveManifestLocked()
	}
	if _, err := s.appendAndCommitModeLocked(leader, newEntry, ballotNormal, activateConfig); err != nil {
		return err
	}
	_ = oldVoters
	return nil
}

type configKind string

const (
	configKindJoint configKind = "joint"
	configKindNew   configKind = "new"
)

// ExpandReplicas grows one shard's replica count online without interrupting
// reads or writes, e.g. 3 voters -> 5 voters.
//
// Safety shape (joint consensus):
//
//  1. New replicas are added as non-voting learners and synchronised to the
//     leader's committed prefix. They serve no reads as voters and cannot
//     influence elections yet.
//  2. A joint configuration entry is committed requiring BOTH an old-config
//     majority AND a new-config majority. Because the old and new
//     majorities overlap (old is a subset of new), two disjoint groups can
//     never each believe they are a majority during the transition.
//  3. Immediately after that commit the new configuration entry is committed
//     under the new quorum alone; from that instant later writes use the new
//     majority threshold, and the on-disk manifest moves to the new
//     membership.
//
// Reads and writes concurrent with the call are serialized on the shard
// lock, so they either observe the old quorum, the joint quorum (stricter),
// or the new quorum — never a moment where an old-only group could commit.
//
// If either commit cannot gather its quorum, no membership change is
// activated: learners remain learners and the call can be retried after
// connectivity is restored.
func (s *ReplicaSet) ExpandReplicas(replicaIDs []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.expandReplicasLocked(replicaIDs)
}

func (s *ReplicaSet) expandReplicasLocked(replicaIDs []string) error {
	if s.joint {
		return newError(KindInvalidConfig, "expand", s.spec.ID,
			"another reconfiguration is already in progress")
	}
	if err := validateNewReplicas(s.order, replicaIDs); err != nil {
		if qerr, ok := err.(*Error); ok {
			qerr.ShardID = s.spec.ID
			qerr.Op = "expand"
		}
		return err
	}

	leader, err := s.ensureLeaderForWriteLocked("expand")
	if err != nil {
		return err
	}
	leader.mu.RLock()
	term := leader.currentTerm
	commitIndex := leader.commitIndex
	leader.mu.RUnlock()

	// Phase 0: materialize new replicas as learners and catch them up to the
	// current committed prefix before they can affect any quorum.
	for _, id := range replicaIDs {
		if _, exists := s.replicas[id]; !exists {
			s.replicas[id] = NewReplica(id, s.spec.ID)
			s.alive[id] = true
			s.order = append(s.order, id)
			s.learners = append(s.learners, id)
			s.elapsedTicks[id] = electionOffsetTicks(id)
			s.logf("shard=%s expansion added learner=%s (non-voting until the joint commit succeeds)",
				s.spec.ID, id)
		}
	}
	for _, id := range replicaIDs {
		if err := s.syncCommittedPrefixLocked(leader, s.replicas[id]); err != nil {
			s.logf("shard=%s expansion pre-sync of learner=%s failed: %v",
				s.spec.ID, id, err)
			return newError(KindReplicaUnavailable, "expand", s.spec.ID,
				"new replica "+id+" could not catch up before becoming a voter")
		}
	}

	oldVoters := append([]string(nil), s.voters...)
	newVoters := append(append([]string(nil), s.voters...), replicaIDs...)
	oldQ, newQ := majorityOf(len(oldVoters)), majorityOf(len(newVoters))
	s.logf("shard=%s expansion begins oldVoters=%v(q=%d) newVoters=%v(q=%d) leader=%s term=%d",
		s.spec.ID, oldVoters, oldQ, newVoters, newQ, leader.id, term)

	// Phase 1: commit the joint entry under BOTH majorities.
	s.oldVoters = oldVoters
	s.newVoters = newVoters
	s.joint = true
	jointIndex := commitIndex + 1
	jointEntry := Entry{
		Index:        jointIndex,
		Term:         term,
		Admin:        true,
		RequestID:    "config-joint",
		ConfigChange: string(configKindJoint),
		OldVoters:    append([]string(nil), oldVoters...),
		NewVoters:    append([]string(nil), newVoters...),
	}
	if _, err := s.appendAndCommitModeLocked(leader, jointEntry, ballotJoint, nil); err != nil {
		// Roll back to the stable old configuration; learners stay in place
		// with only the committed prefix, so a retry does not restart data
		// copying and cannot publish uncommitted data.
		s.joint = false
		s.oldVoters = nil
		s.newVoters = nil
		s.logf("shard=%s expansion aborted at joint commit, old quorum remains authoritative: %v",
			s.spec.ID, err)
		return err
	}

	// Phase 2: from this instant the new quorum governs. Promote learners to
	// voters and commit the new-config entry under the new majority only.
	s.joint = false
	s.voters = newVoters
	s.learners = nil
	s.oldVoters = nil
	s.newVoters = nil
	s.spec.ReplicaIDs = append([]string(nil), newVoters...)

	newEntry := Entry{
		Index:        jointIndex + 1,
		Term:         term,
		Admin:        true,
		RequestID:    "config-new",
		ConfigChange: string(configKindNew),
		NewVoters:    append([]string(nil), newVoters...),
	}
	activateConfig := func() error {
		s.logf("shard=%s joint config committed at index=%d; switching quorum to new voters=%v q=%d",
			s.spec.ID, jointIndex, newVoters, newQ)
		return s.saveManifestLocked()
	}
	if _, err := s.appendAndCommitModeLocked(leader, newEntry, ballotNormal, activateConfig); err != nil {
		// The joint entry is committed but the new entry failed (e.g. one of
		// the freshly added voters crashed exactly now). The shard stays on
		// the joint quorum, which is still safe and stricter than needed;
		// report the failure and keep the explicit state so the next write
		// still requires both halves instead of silently relaxing.
		s.joint = true
		s.oldVoters = oldVoters
		s.newVoters = newVoters
		s.voters = oldVoters
		s.learners = append([]string(nil), replicaIDs...)
		s.spec.ReplicaIDs = append(append([]string(nil), oldVoters...), replicaIDs...)
		s.logf("shard=%s expansion new-config commit failed; remaining in safe joint state: %v",
			s.spec.ID, err)
		return err
	}

	s.logf("shard=%s expansion complete voters=%v quorum=%d effective from index=%d",
		s.spec.ID, s.voters, s.quorumLocked(), jointIndex+1)
	return nil
}
