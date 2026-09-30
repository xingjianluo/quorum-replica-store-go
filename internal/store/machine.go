package store

import "sync"

// StateMachine 是按日志顺序 Apply 的键值状态机。
// 它同时保存"请求标识 -> 已落定结果"，使重复提交返回首次结果而不重复生效。
// CAS 冲突也在 Apply 时按日志顺序串行判定，从而即使并发提交也不会静默覆盖。
type StateMachine struct {
	mu       sync.RWMutex
	data     map[string]*KVItem
	requests map[string]ApplyResult
}

// NewStateMachine 创建空状态机。
func NewStateMachine() *StateMachine {
	return &StateMachine{
		data:     make(map[string]*KVItem),
		requests: make(map[string]ApplyResult),
	}
}

// Apply 按序应用一条日志条目。返回该请求的落定结果。
// 幂等规则（关键不变量）：
//  1. 同一 RequestID 永远只生效一次；重复 Apply 直接返回首次结果，数据不再变更。
//  2. ExpectedVersion >= 0 时做 CAS 检查：当前版本不一致则记 CasConflict，不修改数据。
//  3. 每次成功的 put/del 都让键版本单调 +1（删除保留版本号，形成墓碑）。
func (m *StateMachine) Apply(e Entry) ApplyResult {
	m.mu.Lock()
	defer m.mu.Unlock()

	if e.RequestID != "" {
		if prior, ok := m.requests[e.RequestID]; ok {
			return prior // 已落定：不重复生效
		}
	}

	cur := m.data[e.Key] // 不存在时为 nil，对应版本 0
	var curVersion int64
	if cur != nil {
		curVersion = cur.Version
	}

	// CAS 判定按日志顺序串行执行，避免并发写互相静默覆盖。
	if e.ExpectedVersion >= 0 && e.ExpectedVersion != curVersion {
		res := ApplyResult{OK: false, Version: curVersion, Code: CodeCasConflict}
		if e.RequestID != "" {
			m.requests[e.RequestID] = res
		}
		return res
	}

	newVersion := curVersion + 1
	item := &KVItem{Version: newVersion, LastRequest: e.RequestID}
	if e.Op == OpDel {
		item.Deleted = true
	} else {
		item.Value = append([]byte(nil), e.Value...)
	}
	m.data[e.Key] = item

	res := ApplyResult{OK: true, Version: newVersion}
	if e.RequestID != "" {
		m.requests[e.RequestID] = res
	}
	return res
}

// Get 返回键的当前快照；从未写入或已删除（墓碑）返回 CodeKeyNotFound。
func (m *StateMachine) Get(key string) (KVItem, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	item, ok := m.data[key]
	if !ok || item.Deleted {
		return KVItem{}, newError(CodeKeyNotFound, "key "+quote(key)+" not present")
	}
	return KVItem{
		Value:       append([]byte(nil), item.Value...),
		Version:     item.Version,
		Deleted:     item.Deleted,
		LastRequest: item.LastRequest,
	}, nil
}

// Result 查询某请求标识已落定的结果。
func (m *StateMachine) Result(requestID string) (ApplyResult, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	r, ok := m.requests[requestID]
	return r, ok
}
