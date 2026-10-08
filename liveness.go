package quorum

import "time"

// 后台节拍默认值：心跳/探活每 25ms 一轮；主副本失联后最长 200ms
// （8 个心跳周期）必然触发重新选举，因此无人请求时的接管时间是
// 秒级以下、可预期的。测试可通过 Options 覆盖。
const (
	defaultHeartbeatInterval = 25 * time.Millisecond
	defaultElectionTimeout   = 200 * time.Millisecond
)

// replicaSetTiming 控制无人请求时的心跳节拍与选举超时。
type replicaSetTiming struct {
	heartbeatInterval time.Duration
	electionTimeout   time.Duration
	autoFailover      bool
}

func defaultTiming() replicaSetTiming {
	return replicaSetTiming{
		heartbeatInterval: defaultHeartbeatInterval,
		electionTimeout:   defaultElectionTimeout,
		autoFailover:      true,
	}
}

// Option 在构造期注入诊断、节拍和持久化配置。
type Option func(*ReplicaSet)

// WithTiming 覆盖心跳间隔、选举超时和后台自动接管开关。
func WithTiming(heartbeatInterval, electionTimeout time.Duration, autoFailover bool) Option {
	return func(s *ReplicaSet) {
		if heartbeatInterval > 0 {
			s.timing.heartbeatInterval = heartbeatInterval
		}
		if electionTimeout > 0 {
			s.timing.electionTimeout = electionTimeout
		}
		if !autoFailover {
			s.timing.autoFailover = false
		}
	}
}

func withDurability(d durability) Option {
	return func(s *ReplicaSet) {
		s.durable = d
	}
}

// startFailoverLocked 启动后台探活/选举循环。循环只做两件事：
//   - 确认在任主副本仍能联系投票多数派，能则刷新“最后联系时间”；
//   - 超过选举超时仍没有有效主、且存活投票成员能形成多数派时，自动选主。
//
// 它不复制用户数据，追赶仍由写入路径或 CatchUp 完成，因此不会改变
// 既有测试观察到的副本新鲜度。
func (s *ReplicaSet) startFailoverLocked() {
	if !s.timing.autoFailover {
		return
	}
	s.stopCh = make(chan struct{})
	go func() {
		ticker := time.NewTicker(s.timing.heartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-s.stopCh:
				return
			case now := <-ticker.C:
				s.mu.Lock()
				s.tickLocked(now)
				s.mu.Unlock()
			}
		}
	}()
}

// Close stops the background failover loop. It is idempotent.
func (s *ReplicaSet) Close() {
	s.mu.Lock()
	ch := s.stopCh
	s.mu.Unlock()
	if ch != nil {
		s.stopOnce.Do(func() { close(ch) })
	}
}

// tickLocked 是后台循环的一次确定性节拍，必须持锁调用。
func (s *ReplicaSet) tickLocked(now time.Time) {
	if s.leaderID != "" {
		leader := s.replicas[s.leaderID]
		reachable := s.reachableVotersLocked(s.leaderID)
		newerTerm := s.maxTermAmongLocked(reachable)
		if s.alive[s.leaderID] && len(reachable) >= s.quorumLocked() && newerTerm <= leader.currentTerm {
			s.lastLeaderContact = now
			return
		}
		// 主进程崩溃、被分区到少数派，或出现更高任期：立即失效旧主。
		s.invalidateStaleLeaderLocked("heartbeat-timeout")
		s.leaderID = ""
	}

	if s.aliveVotersLocked() < s.quorumLocked() {
		// 凑不出多数派就明确停在无主、不可写状态；不重试无意义的选举。
		return
	}
	if s.lastLeaderContact.IsZero() {
		s.lastLeaderContact = now
	}
	if now.Sub(s.lastLeaderContact) < s.timing.electionTimeout {
		return
	}
	s.logf("shard=%s heartbeat election after timeout=%s without a confirmed leader",
		s.spec.ID, s.timing.electionTimeout)
	if _, err := s.electLeaderLocked(""); err != nil {
		s.logf("shard=%s heartbeat election failed, will retry next tick: %v",
			s.spec.ID, err)
		return
	}
	s.lastLeaderContact = now
}
