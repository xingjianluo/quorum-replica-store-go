package quorum

// restoreLocked rebuilds durable membership and replica state from the data
// directory. It is invoked while a ReplicaSet is being constructed:
//
//   - With no manifest, the shard starts fresh from its initial specification.
//   - With a manifest, membership comes from disk; this is how replicas added
//     by an online expansion survive a full restart.
//   - Each replica snapshot contains only its committed prefix, so replays
//     reproduce exactly the committed versions and request-ID results.
//
// A joint configuration is never persisted as committed: if a restart
// interrupts an expansion, the shard comes back in the last stable
// configuration and the change can simply be retried.
func (s *ReplicaSet) restoreLocked() error {
	if s.persister == nil || !s.persister.enabled() {
		return nil
	}
	manifest, ok, err := s.persister.loadManifest(s.spec.ID)
	if err != nil {
		return err
	}
	if ok {
		if len(manifest.Voters) < 2 {
			return newError(KindInvalidConfig, "restore", s.spec.ID,
				"durable manifest has fewer than two voters")
		}
		seen := map[string]bool{}
		for _, id := range manifest.Voters {
			if seen[id] {
				return newError(KindInvalidConfig, "restore", s.spec.ID, "duplicate durable voter "+id)
			}
			seen[id] = true
			if _, exists := s.replicas[id]; !exists {
				s.replicas[id] = NewReplica(id, s.spec.ID)
			}
			s.alive[id] = true
		}
		for _, id := range manifest.Learners {
			if seen[id] {
				return newError(KindInvalidConfig, "restore", s.spec.ID,
					"durable learner "+id+" is also a voter")
			}
			seen[id] = true
			if _, exists := s.replicas[id]; !exists {
				s.replicas[id] = NewReplica(id, s.spec.ID)
			}
			s.alive[id] = true
		}
		s.voters = append([]string(nil), manifest.Voters...)
		s.learners = append([]string(nil), manifest.Learners...)
		s.joint = false
		s.oldVoters = nil
		s.newVoters = nil
		s.order = append(append([]string(nil), s.voters...), s.learners...)
		s.spec.ReplicaIDs = append([]string(nil), s.order...)
		s.logf("shard=%s restored membership from durable manifest voters=%v learners=%v",
			s.spec.ID, s.voters, s.learners)
	}

	for _, id := range s.order {
		replica := s.replicas[id]
		snap, has, err := s.persister.loadReplica(s.spec.ID, id)
		if err != nil {
			return err
		}
		if !has {
			continue
		}
		replica.mu.Lock()
		if err := replica.installSnapshotLocked(snap); err != nil {
			replica.mu.Unlock()
			return err
		}
		s.elapsedTicks[id] = electionOffsetTicks(id)
		replica.mu.Unlock()
		s.logf("shard=%s restored replica=%s term=%d commit=%d entries=%d from durable snapshot",
			s.spec.ID, id, snap.CurrentTerm, snap.CommitIndex, len(snap.Entries))
	}
	return nil
}
