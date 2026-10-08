package quorum

// WriteState is the coarse machine-readable writability verdict of a shard.
type WriteState string

const (
	// WriteStateWritable means a confirmed leader currently has a quorum.
	WriteStateWritable WriteState = "writable"
	// WriteStateNoLeader means no leader is confirmed and a new one must be
	// elected (or an election is in progress).
	WriteStateNoLeader WriteState = "no_leader"
	// WriteStateNoQuorum means a leader exists or existed, but a live voting
	// majority cannot currently be formed; writes must stop rather than
	// degrade.
	WriteStateNoQuorum WriteState = "no_quorum"
	// WriteStateReconfiguring means a joint configuration change is in
	// progress; both old and new majorities gate writes.
	WriteStateReconfiguring WriteState = "reconfiguring"
)

// ShardStatus is the honest external view of one shard's ability to serve.
//
// Writable is derived from live state, never from a stale leader record:
// if the recorded leader cannot reach a voting majority the status reports
// the blocking reason instead of pretending it still serves.
type ShardStatus struct {
	ShardID          string
	LeaderID         string
	Term             uint64
	Writable         bool
	WriteState       WriteState
	WriteBlockReason string
	ConfiguredVoters []string
	Learners         []string
	AliveCount       int
	Quorum           int
	Joint            bool
	OldVoters        []string
	NewVoters        []string
	OldQuorum        int
	NewQuorum        int
	LeaderReachable  int
}

// ClusterStatus evaluates current writability without mutating anything: it
// does not trigger an election. Reads and writes may still repair a missing
// leader; this snapshot only describes the instant it is taken.
func (s *ReplicaSet) ClusterStatus() ShardStatus {
	s.mu.Lock()
	defer s.mu.Unlock()

	status := ShardStatus{
		ShardID:          s.spec.ID,
		ConfiguredVoters: append([]string(nil), s.voters...),
		Learners:         append([]string(nil), s.learners...),
		AliveCount:       len(s.aliveVotersLocked()),
		Quorum:           s.quorumForLocked(ballotNormal),
		Joint:            s.joint,
		OldVoters:        append([]string(nil), s.oldVoters...),
		NewVoters:        append([]string(nil), s.newVoters...),
	}

	if s.leaderID != "" {
		leader := s.replicas[s.leaderID]
		leader.mu.RLock()
		status.LeaderID = leader.id
		status.Term = leader.currentTerm
		leader.mu.RUnlock()
		status.LeaderReachable = len(s.reachableMembersLocked(leader.id, s.voters))
	}

	switch {
	case s.joint:
		status.WriteState = WriteStateReconfiguring
		status.OldQuorum, status.NewQuorum = s.jointQuorumsLocked()
		// Joint writes need both halves; the verdict checks them separately
		// so the reason string says exactly which side is short.
		if s.leaderID == "" || !s.alive[s.leaderID] {
			status.WriteBlockReason = "joint change in progress with no confirmed leader"
			return status
		}
		oldReach, newReach := 0, 0
		leader := s.leaderID
		for _, id := range s.oldVoters {
			if s.reachableLocked(leader, id) {
				oldReach++
			}
		}
		for _, id := range s.newVoters {
			if s.reachableLocked(leader, id) {
				newReach++
			}
		}
		if oldReach >= status.OldQuorum && newReach >= status.NewQuorum {
			status.Writable = true
			return status
		}
		status.WriteBlockReason = "joint quorum not reachable: " +
			"old=" + itoa(oldReach) + "/" + itoa(status.OldQuorum) +
			" new=" + itoa(newReach) + "/" + itoa(status.NewQuorum)
	case status.AliveCount < status.Quorum:
		// A live majority is impossible right now. The internal record may
		// still name the previous leader for a tick, but the external view
		// must not advertise it as serving; expose a stale leader only when
		// it is still alive (it may serve stale eventual reads).
		if s.leaderID != "" && !s.alive[s.leaderID] {
			status.LeaderID = ""
			status.Term = 0
		}
		status.WriteState = WriteStateNoQuorum
		status.WriteBlockReason = "alive voting replicas below majority: alive=" +
			itoa(status.AliveCount) + " quorum=" + itoa(status.Quorum)
	case s.leaderID == "":
		status.WriteState = WriteStateNoLeader
		status.WriteBlockReason = "no confirmed leader; an automatic election will run at the next timeout"
	case !s.alive[s.leaderID]:
		status.WriteState = WriteStateNoLeader
		status.WriteBlockReason = "recorded leader " + s.leaderID + " is crashed; awaiting automatic takeover"
	case status.LeaderReachable < status.Quorum:
		status.WriteState = WriteStateNoQuorum
		status.WriteBlockReason = "leader " + s.leaderID + " reaches only " +
			itoa(status.LeaderReachable) + " voters, quorum=" + itoa(status.Quorum)
	default:
		status.WriteState = WriteStateWritable
		status.Writable = true
	}
	return status
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// ClusterStatus reports the serving verdict of every shard in range order.
func (s *Store) ClusterStatus() []ShardStatus {
	out := make([]ShardStatus, 0, len(s.order))
	for _, id := range s.order {
		out = append(out, s.sets[id].ClusterStatus())
	}
	return out
}
