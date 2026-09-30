package store

import "sync"

// StateMachine 是按日志顺序 Apply 的键值状态机。
// 它同时保存"请求标识 -> 已落定结果"，使重复提交返回首次结果而不重复生效。
type StateMachine struct {
	mu       sync.RWMutex
	data     map[string]*KVItem
	requests map[string]ApplyResult
}

// NewStateMachine 创建空状态机。
func NewStateMachine() *StateMachine {
	// TODO(impl)
	return &StateMachine{}
}

// Apply 按序应用一条已提交日志。
// 若条目的 RequestID 已经应用过，则直接返回首次结果（幂等）。
func (m *StateMachine) Apply(e Entry) ApplyResult {
	// TODO(impl)
	return ApplyResult{}
}

// Get 返回键的当前快照；不存在（或已删除）返回 CodeKeyNotFound。
func (m *StateMachine) Get(key string) (KVItem, error) {
	// TODO(impl)
	return KVItem{}, nil
}

// Result 查询某请求标识已落定的结果。
func (m *StateMachine) Result(requestID string) (ApplyResult, bool) {
	// TODO(impl)
	return ApplyResult{}, false
}
