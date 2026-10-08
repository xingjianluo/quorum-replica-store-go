package quorum

// LeaderStatus 是“当前谁是主、能不能写、为什么不能写”的只读诊断快照。
// 它不触发选举、不改变任何状态，适合无人值守时的对外健康查询。
type LeaderStatus struct {
	ShardID string
	// LeaderID 是当前在任主副本；没有合法主时为空。
	LeaderID string
	// Term 是在任主的任期；无主时为 0。
	Term uint64
	// Writable 报告当前是否存在一个经多数派确认、任期最新的主。
	Writable bool
	// Reason 在 Writable=false 时给出稳定的机器可读原因：
	//   - quorum_unavailable：存活投票成员凑不出多数派；
	//   - no_leader：没有主，且当前也无法形成多数派之外的合法主；
	//   - not_leader：主记录存在但已失联/被更高任期取代；
	//   - strong_read_unsafe：主存活但联系不到投票多数派。
	Reason ErrorKind
	// VoterCount 是当前配置中的投票成员总数（不含追赶中的学习者）。
	VoterCount int
	// AliveVoters 是当前未崩溃的投票成员数。
	AliveVoters int
	// Quorum 是当前多数派门槛；扩容提交后立即按新配置计算。
	Quorum int
	// LeaderReachable 是在任主当前可达的投票成员数；无主时为 0。
	LeaderReachable int
}

// Leadership 返回不产生副作用的主副本可服务性判定。
func (s *ReplicaSet) Leadership() LeaderStatus {
	s.mu.Lock()
	defer s.mu.Unlock()

	voters := s.votersLocked()
	status := LeaderStatus{
		ShardID:     s.spec.ID,
		VoterCount:  len(voters),
		AliveVoters: s.aliveVotersLocked(),
		Quorum:      s.quorumLocked(),
	}

	if status.AliveVoters < status.Quorum {
		// 存活成员本身凑不够多数派：明确停在不可写，绝不宣称旧主在服务。
		status.Reason = KindQuorumUnavailable
		if s.leaderID != "" {
			status.LeaderID = s.leaderID
			leader := s.replicas[s.leaderID]
			leader.mu.RLock()
			status.Term = leader.currentTerm
			leader.mu.RUnlock()
		}
		s.logf("shard=%s leadership report: not writable, aliveVoters=%d/%d quorum=%d",
			s.spec.ID, status.AliveVoters, status.VoterCount, status.Quorum)
		return status
	}

	if s.leaderID == "" {
		status.Reason = KindNoLeader
		s.logf("shard=%s leadership report: no leader yet, aliveVoters=%d quorum=%d",
			s.spec.ID, status.AliveVoters, status.Quorum)
		return status
	}

	leader := s.replicas[s.leaderID]
	leader.mu.RLock()
	status.Term = leader.currentTerm
	leader.mu.RUnlock()
	status.LeaderID = s.leaderID

	if !s.alive[s.leaderID] {
		status.Reason = KindNotLeader
		s.logf("shard=%s leadership report: recorded leader=%s is crashed, not writable",
			s.spec.ID, s.leaderID)
		return status
	}

	reachable := s.reachableVotersLocked(s.leaderID)
	status.LeaderReachable = len(reachable)
	if len(reachable) < status.Quorum {
		status.Reason = KindStrongReadUnsafe
		s.logf("shard=%s leadership report: leader=%s partitioned, reachableVoters=%d quorum=%d",
			s.spec.ID, s.leaderID, len(reachable), status.Quorum)
		return status
	}
	if newer := s.maxTermAmongLocked(reachable); newer > status.Term {
		status.Reason = KindNotLeader
		s.logf("shard=%s leadership report: leader=%s term=%d superseded by term=%d",
			s.spec.ID, s.leaderID, status.Term, newer)
		return status
	}

	status.Writable = true
	return status
}

// Leadership 返回所有分片的主副本诊断快照，键为分片 ID。
func (s *Store) Leadership() map[string]LeaderStatus {
	out := make(map[string]LeaderStatus, len(s.order))
	for _, id := range s.order {
		out[id] = s.sets[id].Leadership()
	}
	return out
}
