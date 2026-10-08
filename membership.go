package quorum

import (
	"slices"
)

// ConfigChangeType 标识成员变更条目的类型。
type ConfigChangeType int

const (
	// ConfigChangeAddReplica 表示向分片增加一个投票副本。
	ConfigChangeAddReplica ConfigChangeType = iota + 1
)

// ConfigChange 是日志中的成员变更载荷。它通过普通的多数派复制提交，
// 因此成员变更和用户写入共用同一条日志顺序，不会出现配置跳跃。
type ConfigChange struct {
	Type      ConfigChangeType `json:"type"`
	ReplicaID string           `json:"replica_id"`
}

// ExpandReplicaSet 在不中断读写的前提下，把新副本逐个加入投票集合。
//
// 安全性依据：每次只增加一个投票成员。设旧配置为 C_old、新配置为
// C_new=C_old+1，则任意两个多数派必有交集：
//
//	3->4: 旧多数派 2/3，新多数派 3/4，任取两组至少共享 1 个节点；
//	4->5: 旧多数派 3/4，新多数派 3/5，任取两组至少共享 1 个节点。
//
// 因此扩容期间不可能出现两拨副本各自凑够多数派、各自提交冲突结果。
//
// 每个新副本的流程：
//  1. 以学习者身份加入成员列表，但不投票、不计入多数派；
//  2. 从当前主同步完整的已提交前缀（只复制已确认数据）；
//  3. 追加一条配置变更日志条目，按旧配置多数派复制并提交；
//  4. 提交点落盘多数派后，学习者原子地转为正式投票成员；
//     下一条写入立刻使用新的多数派门槛。
//
// 中途随整套实例停止时：未提交的配置条目在重启时被丢弃，系统回到最后
// 一次已提交的配置；已提交到多数派的配置则随日志恢复并继续生效。
func (s *ReplicaSet) ExpandReplicaSet(replicaIDs []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.expandLocked(replicaIDs)
}

func (s *ReplicaSet) expandLocked(replicaIDs []string) error {
	for _, id := range replicaIDs {
		if id == "" {
			return newError(KindConfigChange, "expand", s.spec.ID, "new replica id is empty")
		}
		if err := s.requireReplicaLocked(id); err == nil {
			return newError(KindConfigChange, "expand", s.spec.ID,
				"replica "+id+" is already a member (possibly still a learner)")
		}
	}

	for _, id := range replicaIDs {
		if err := s.addOneReplicaLocked(id); err != nil {
			return err
		}
	}
	return nil
}

func (s *ReplicaSet) addOneReplicaLocked(id string) error {
	leader, err := s.ensureLeaderForWriteLocked("expand")
	if err != nil {
		return err
	}

	// 以学习者身份加入：可被复制和追赶，但不投票、不计入多数派。
	learner := NewReplica(id, s.spec.ID)
	s.replicas[id] = learner
	s.order = append(s.order, id)
	s.alive[id] = true
	s.learners[id] = true
	s.logf("shard=%s expansion starts: learner=%s attached, voters=%d quorum=%d (new node not counted)",
		s.spec.ID, id, len(s.votersLocked()), s.quorumLocked())

	// 学习者必须先收敛到当前已提交前缀；它永远不会带着空日志直接获得
	// 投票权，避免一个落后节点当选后截断已提交数据。
	if syncErr := s.syncCommittedPrefixLocked(leader, learner); syncErr != nil {
		s.removeLearnerLocked(id)
		return newError(KindConfigChange, "expand", s.spec.ID,
			"new replica "+id+" cannot catch up the committed prefix before joining").wrap(syncErr)
	}

	leader.mu.RLock()
	nextIndex := leader.lastIndexLocked() + 1
	term := leader.currentTerm
	leader.mu.RUnlock()
	entry := Entry{
		Index:     nextIndex,
		Term:      term,
		Admin:     true,
		RequestID: "config:add:" + id,
		ConfigChange: &ConfigChange{
			Type:      ConfigChangeAddReplica,
			ReplicaID: id,
		},
	}
	if _, appendErr := s.appendAndCommitLocked(leader, entry); appendErr != nil {
		s.removeLearnerLocked(id)
		s.logf("shard=%s expansion of replica=%s rolled back: config entry not committed: %v",
			s.spec.ID, id, appendErr)
		return newError(KindConfigChange, "expand", s.spec.ID,
			"config entry adding replica "+id+" was not committed by the current majority").wrap(appendErr)
	}
	// appendAndCommitLocked 已在提交并落盘后调用 applyConfigChangeLocked，
	// 学习者此刻已是正式投票成员，s.spec.ReplicaIDs 也已包含新节点。
	if s.learners[id] || !slices.Contains(s.spec.ReplicaIDs, id) {
		s.removeLearnerLocked(id)
		return newError(KindConfigChange, "expand", s.spec.ID,
			"internal error: config entry committed but replica "+id+" was not activated")
	}
	s.logf("shard=%s expansion committed: replica=%s is now a voter, voters=%d quorum=%d",
		s.spec.ID, id, len(s.votersLocked()), s.quorumLocked())
	return nil
}

// removeLearnerLocked undoes an attachment whose config entry did not commit.
// No committed state references the replica yet, so dropping it is safe.
func (s *ReplicaSet) removeLearnerLocked(id string) {
	delete(s.replicas, id)
	delete(s.alive, id)
	delete(s.learners, id)
	s.order = slices.DeleteFunc(s.order, func(existing string) bool { return existing == id })
}

// ExpandShard 对指定分片执行在线扩容，并同步更新 Store 的配置视图。
// Store 持有的 specs 只用于新 Store 的启动配置；运行期成员以各副本集已
// 提交的配置为准。
func (s *Store) ExpandShard(shardID string, replicaIDs []string) error {
	set, err := s.Shard(shardID)
	if err != nil {
		return err
	}
	if err := set.ExpandReplicaSet(replicaIDs); err != nil {
		return err
	}
	newSpec := set.Spec()
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.specs {
		if s.specs[i].ID == shardID {
			s.specs[i].ReplicaIDs = append([]string(nil), newSpec.ReplicaIDs...)
			return nil
		}
	}
	return nil
}
