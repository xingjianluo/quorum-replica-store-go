package store

import (
	"sync"
)

// Op 表示已提交日志条目里的操作类型。
type Op string

const (
	OpPut Op = "put" // 写入/覆盖一个键的版本
	OpDel Op = "del" // 删除一个键
)

// Entry 是一条持久化的复制日志条目。
// Term/Index 是 Raft 风格的任期与日志位置；RequestID 携带客户端请求标识，
// 状态机在 Apply 时据此保证同一请求只生效一次。
type Entry struct {
	Term        int64
	Index       int64
	Op          Op
	Key         string
	Value       []byte
	Version     int64 // 本条目写入后键的新版本号；删除表示版本仍递增
	RequestID   string
}

// KVItem 是某个键在状态机中的当前快照。
type KVItem struct {
	Value       []byte
	Version     int64
	Deleted     bool
	LastRequest string // 最近一次生效的请求标识（幂等去重依据）
}

// ApplyResult 是某个请求标识在状态机中已落定的结果。
// 节点重启/主切换后仍可凭持久日志重建它，保证"重复请求不重复生效"。
type ApplyResult struct {
	OK      bool
	Version int64
	Code    string // 若首次执行被拒（如 CasConflict），记录原因码
}

// DurableLog 是节点本地持久日志的抽象。
// 本实现采用互斥保护的内存切片并落盘到文件（重启后保留），
// 以演示"崩溃不丢已提交数据、长时间离线副本日志落后后再追赶"。
type DurableLog struct {
	mu      sync.Mutex
	path    string
	entries []Entry
	// TODO(impl): 文件持久化字段
}

// NewDurableLog 打开（或创建）path 下的持久日志并加载已有条目。
func NewDurableLog(path string) (*DurableLog, error) {
	// TODO(impl)
	return &DurableLog{path: path}, nil
}

// Entries 返回 [from, to) 的日志条目副本。
func (l *DurableLog) Entries(from, to int64) []Entry {
	// TODO(impl)
	return nil
}

// EntryAt 返回指定下标的条目。
func (l *DurableLog) EntryAt(index int64) (Entry, bool) {
	// TODO(impl)
	return Entry{}, false
}

// Last 返回最后一条日志的任期与下标。
func (l *DurableLog) Last() (term, index int64) {
	// TODO(impl)
	return 0, 0
}

// Append 追加一批日志（下标必须连续且接在当前末尾之后）。
func (l *DurableLog) Append(entries ...Entry) error {
	// TODO(impl)
	return nil
}

// Truncate 截断到 index（保留 <=index 的条目），用于解决冲突日志。
func (l *DurableLog) Truncate(index int64) error {
	// TODO(impl)
	return nil
}

// Close 关闭日志并刷盘。
func (l *DurableLog) Close() error {
	// TODO(impl)
	return nil
}
