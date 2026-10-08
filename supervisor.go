package quorum

import "time"

// Supervisor timing, expressed in ticks so it is independent of the configured
// tick interval. The leader confirms liveness every tick; a follower starts
// an election after missing enough confirmations. Timeouts are deterministically
// staggered by the replica's position, so exactly one follower times out
// first and split elections do not depend on random timers.
const (
	heartbeatTicks       = 1
	electionBaseTicks    = 10
	electionStaggerTicks = 4
)

// electionTimeoutTicks is the per-replica election timeout. With a 50ms tick
// the fastest follower campaigns after about 500ms, the next 200ms later,
// making takeover predictable and sub-second on the common path.
func electionTimeoutTicks(orderIndex int) int {
	return electionBaseTicks + orderIndex*electionStaggerTicks
}

// electionOffsetTicks seeds a freshly restored/recovered replica partway to
// its timeout, so a simultaneously restarted cluster converges quickly but
// still in the same deterministic order.
func electionOffsetTicks(id string) int {
	return len(id) % electionBaseTicks
}

func (s *ReplicaSet) orderIndexLocked(id string) int {
	for i, known := range s.order {
		if known == id {
			return i
		}
	}
	return 0
}

// isVoterLocked reports whether id may vote and count toward elections.
// Learners catch up data but never vote.
func (s *ReplicaSet) isVoterLocked(id string) bool {
	for _, voter := range s.voters {
		if voter == id {
			return true
		}
	}
	return false
}

// startSupervisorLocked launches the background ticker. The caller holds mu
// (or runs during construction) and must have set stopCh and tickInterval.
func (s *ReplicaSet) startSupervisorLocked() {
	stop := s.stopCh
	interval := s.tickInterval
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				s.mu.Lock()
				s.tickLocked()
				s.mu.Unlock()
			}
		}
	}()
}

// closeSupervisor stops the ticker and waits for it to finish. It is safe to
// call more than once and before the supervisor ever started.
func (s *ReplicaSet) closeSupervisor() {
	s.mu.Lock()
	stop := s.stopCh
	s.stopCh = nil
	running := s.running
	s.running = false
	s.mu.Unlock()
	if stop != nil {
		close(stop)
	}
	if running {
		s.wg.Wait()
	}
}

// aliveVotersLocked lists voting replicas that are currently alive.
func (s *ReplicaSet) aliveVotersLocked() []string {
	out := make([]string, 0, len(s.voters))
	for _, id := range s.voters {
		if s.alive[id] {
			out = append(out, id)
		}
	}
	return out
}

// tickLocked performs one deterministic supervision step:
//
//  1. If a leader is recorded, confirm it still reaches a live voting
//     majority and that no reachable peer carries a higher term. If not,
//     step it down immediately: the shard must not keep advertising a leader
//     that cannot serve, and it must not degrade writes.
//  2. A confirmed leader heartbeats every reachable voter, proving liveness
//     and resetting each follower's election timer.
//  3. Each other alive voter whose timer reached its deterministic timeout
//     runs one election attempt. Staggered offsets mean the most up-to-date
//     reachable follower normally wins on the first round; a later follower
//     campaigns in a subsequent round if the first attempt could not gather
//     a majority.
func (s *ReplicaSet) tickLocked() {
	if len(s.voters) == 0 {
		return
	}

	// Recorded leader liveness confirmation. An isolated-but-alive leader
	// keeps its local Leader role (Raft: it cannot learn of a higher term
	// until reachability returns), but the shard must stop advertising it:
	// every write path independently rechecks the reachable voting quorum, so
	// such a leader can never commit while isolated.
	if s.leaderID != "" {
		leader := s.replicas[s.leaderID]
		leaderReachableVoters := 0
		for _, id := range s.voters {
			if s.reachableLocked(s.leaderID, id) {
				leaderReachableVoters++
			}
		}
		hasQuorum := s.alive[s.leaderID] && leaderReachableVoters >= s.quorumLocked()
		newerTermSeen := s.maxTermAmongLocked(s.reachableFromLocked(s.leaderID)) > s.currentTermLocked(leader)
		if hasQuorum && !newerTermSeen {
			// Heartbeat: reset timers of every reachable alive voter.
			for _, id := range s.voters {
				if id == s.leaderID {
					continue
				}
				if s.reachableLocked(s.leaderID, id) {
					s.elapsedTicks[id] = 0
				}
			}
			s.logf("shard=%s heartbeat leader=%s term=%d voterReach=%d/%d",
				s.spec.ID, leader.id, s.currentTermLocked(leader),
				leaderReachableVoters, s.quorumLocked())
			return
		}
		s.logf("shard=%s supervision steps down leader=%s alive=%t voterReach=%d/%d",
			s.spec.ID, s.leaderID, s.alive[s.leaderID], leaderReachableVoters, s.quorumLocked())
		s.invalidateStaleLeaderLocked("heartbeat-majority-lost")
	}

	// No confirmed leader: advance election timers of alive voters.
	for _, id := range s.voters {
		if !s.alive[id] {
			continue
		}
		if s.leaderID != "" {
			// A different leader may have been elected earlier in this tick.
			if s.reachableLocked(id, s.leaderID) {
				continue
			}
		}
		s.elapsedTicks[id]++
		replica := s.replicas[id]
		replica.mu.RLock()
		state := replica.state
		replica.mu.RUnlock()
		timeout := electionTimeoutTicks(s.orderIndexLocked(id))
		if s.elapsedTicks[id] < timeout && state != ReplicaStateCandidate {
			continue
		}
		// Only a replica that can itself see a voting majority should
		// campaign; otherwise the attempt cannot succeed and must not burn
		// terms while the shard is genuinely without a quorum.
		reachableVoters := 0
		for _, voter := range s.voters {
			if s.reachableLocked(id, voter) {
				reachableVoters++
			}
		}
		if reachableVoters < s.quorumLocked() {
			s.logf("shard=%s election timer fired for=%s reachableVoters=%d/%d: cannot campaign, shard stays not-writable",
				s.spec.ID, id, reachableVoters, s.quorumLocked())
			// Keep the timer from spinning every tick while isolated.
			s.elapsedTicks[id] = timeout
			continue
		}
		s.logf("shard=%s election timeout for=%s elapsed=%d timeout=%d orderIndex=%d",
			s.spec.ID, id, s.elapsedTicks[id], timeout, s.orderIndexLocked(id))
		term := s.maxAliveTermLocked() + 1
		if s.tryCandidateLocked(id, term) {
			s.confirmLeaderTimersLocked(id)
			return
		}
		// Failed attempt: the replica reverts to follower inside the next
		// successful campaign; reset its timer to the staggered slot so the
		// next round remains deterministic rather than busy-looping terms.
		s.elapsedTicks[id] = 0
		replica.mu.Lock()
		replica.state = ReplicaStateFollower
		replica.mu.Unlock()
	}
}

func (s *ReplicaSet) currentTermLocked(r *Replica) uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.currentTerm
}

// confirmLeaderTimersLocked marks every reachable follower as freshly
// contacted immediately after an automatic election.
func (s *ReplicaSet) confirmLeaderTimersLocked(leaderID string) {
	for _, id := range s.voters {
		if id == leaderID {
			continue
		}
		if s.reachableLocked(leaderID, id) {
			s.elapsedTicks[id] = 0
		}
	}
}
