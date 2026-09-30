package store

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
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
	Term      int64  `json:"term"`
	Index     int64  `json:"index"`
	Op        Op     `json:"op"`
	Key       string `json:"key"`
	Value     []byte `json:"value,omitempty"`
	Version   int64  `json:"version"`
	ExpectedVersion int64 `json:"expected_version"`
	RequestID string `json:"request_id,omitempty"`
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
// 采用互斥保护的内存索引 + JSON-lines 追加文件（每次追加 fsync），
// 以演示"崩溃不丢已提交数据、长时间离线副本日志落后后再追赶"。
type DurableLog struct {
	mu      sync.Mutex
	path    string
	entries []Entry
}

// NewDurableLog 打开（或创建）path 下的持久日志并加载已有条目。
// path 为空时退化为纯内存日志（仅特殊测试需要）。
func NewDurableLog(path string) (*DurableLog, error) {
	l := &DurableLog{path: path}
	if path == "" {
		return l, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, wrapError(CodeUnavailable, "create log dir for "+path, err)
	}
	f, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE, 0o644)
	if err != nil {
		return nil, wrapError(CodeUnavailable, "open log "+path, err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var e Entry
		if err := json.Unmarshal(line, &e); err != nil {
			return nil, wrapError(CodeUnavailable, "corrupt log entry in "+path, err)
		}
		l.entries = append(l.entries, e)
	}
	if err := sc.Err(); err != nil {
		return nil, wrapError(CodeUnavailable, "scan log "+path, err)
	}
	return l, nil
}

// Entries 返回 [from, to) 的日志条目副本；to<=0 表示到末尾。越界自动收敛。
func (l *DurableLog) Entries(from, to int64) []Entry {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := int64(len(l.entries))
	if from < 1 {
		from = 1
	}
	if to <= 0 || to > n {
		to = n
	}
	if from > to {
		return nil
	}
	out := make([]Entry, to-from)
	copy(out, l.entries[from-1:to])
	return out
}

// EntryAt 返回指定下标的条目（下标从 1 开始）。
func (l *DurableLog) EntryAt(index int64) (Entry, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if index < 1 || index > int64(len(l.entries)) {
		return Entry{}, false
	}
	return l.entries[index-1], true
}

// Last 返回最后一条日志的任期与下标；空日志返回 (0,0)。
func (l *DurableLog) Last() (term, index int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.entries) == 0 {
		return 0, 0
	}
	e := l.entries[len(l.entries)-1]
	return e.Term, e.Index
}

// Append 追加一批日志（下标必须连续且接在当前末尾之后）并 fsync。
func (l *DurableLog) Append(entries ...Entry) error {
	if len(entries) == 0 {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	next := int64(len(l.entries)) + 1
	for _, e := range entries {
		if e.Index != next {
			return newError(CodeUnavailable,
				"non-contiguous append: expected index "+itoa64(next)+", got "+itoa64(e.Index))
		}
		next++
	}
	if l.path != "" {
		f, err := os.OpenFile(l.path, os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			return wrapError(CodeUnavailable, "open append log "+l.path, err)
		}
		bw := bufio.NewWriter(f)
		enc := json.NewEncoder(bw)
		for _, e := range entries {
			if err := enc.Encode(e); err != nil {
				f.Close()
				return wrapError(CodeUnavailable, "encode log entry", err)
			}
		}
		if err := bw.Flush(); err != nil {
			f.Close()
			return wrapError(CodeUnavailable, "flush log", err)
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return wrapError(CodeUnavailable, "fsync log", err)
		}
		if err := f.Close(); err != nil {
			return wrapError(CodeUnavailable, "close log", err)
		}
	}
	l.entries = append(l.entries, entries...)
	return nil
}

// Truncate 截断到 keepIndex（保留 <=keepIndex 的条目），用于解决冲突日志，并重写落盘文件。
func (l *DurableLog) Truncate(keepIndex int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if keepIndex < 0 {
		keepIndex = 0
	}
	if int(keepIndex) > len(l.entries) {
		return nil
	}
	l.entries = l.entries[:keepIndex]
	if l.path == "" {
		return nil
	}
	tmp := l.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return wrapError(CodeUnavailable, "rewrite log", err)
	}
	bw := bufio.NewWriter(f)
	enc := json.NewEncoder(bw)
	for _, e := range l.entries {
		if err := enc.Encode(e); err != nil {
			f.Close()
			return wrapError(CodeUnavailable, "encode truncated log", err)
		}
	}
	if err := bw.Flush(); err != nil {
		f.Close()
		return wrapError(CodeUnavailable, "flush truncated log", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return wrapError(CodeUnavailable, "fsync truncated log", err)
	}
	if err := f.Close(); err != nil {
		return wrapError(CodeUnavailable, "close truncated log", err)
	}
	if err := os.Rename(tmp, l.path); err != nil {
		return wrapError(CodeUnavailable, "rename truncated log", err)
	}
	return nil
}

// Close 释放日志资源（本实现无驻留句柄，保留接口语义）。
func (l *DurableLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return nil
}


func itoa64(i int64) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [24]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		p--
		b[p] = '-'
	}
	return string(b[p:])
}
