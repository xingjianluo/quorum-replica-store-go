package store

import (
	"sync"
	"time"
)

// WriteRequest 是客户端写请求；RequestID 非空时启用幂等去重。
// ExpectedVersion >= 0 时启用 CAS 语义：与当前版本不一致则拒绝（版本冲突）。
type WriteRequest struct {
	ShardID         int
	Op              Op
	Key             string
	Value           []byte
	ExpectedVersion int64 // -1 表示不做版本检查
	RequestID       string
}

// WriteResponse 是写结果；FromRequestID 回显请求标识，便于客户端识别幂等响应。
type WriteResponse struct {
	Version       int64
	Leader        string
	FromRequestID string
}

// ReadRequest 是客户端读请求。
type ReadRequest struct {
	ShardID int
	Key     string
	Mode    ReadMode
}

// ReadResponse 是读结果；Stale=true 表示最终一致读到的是可能落后的副本。
type ReadResponse struct {
	Item   KVItem
	From   string
	Stale  bool
	Leader string
}

// Cluster 组装路由、所有分片副本与故障注入网络，
// 并实现节点间 Transport 与面向客户端的统一入口。
type Cluster struct {
	mu      sync.RWMutex
	router  *Router
	net     *Network
	nodes   map[string]*Node // 节点ID -> 节点
	owner   map[string]int   // 节点ID -> 分片ID
	logger  Logger
	dataDir string
	tick    time.Duration
	stopCh  chan struct{}
	running bool
}

// NewCluster 创建集群。dataDir 非空时节点持久日志写入该目录下按节点命名的子目录。
func NewCluster(router *Router, net *Network, dataDir string, logger Logger) *Cluster {
	// TODO(impl)
	return &Cluster{}
}

// AddReplica 为某分片构造并登记一个副本节点。
func (c *Cluster) AddReplica(shardID int, id string) error {
	// TODO(impl)
	return nil
}

// StartAll 启动全部节点的驱动循环并返回。
func (c *Cluster) StartAll() {
	// TODO(impl)
}

// ShutdownAll 停止全部节点驱动。
func (c *Cluster) ShutdownAll() {
	// TODO(impl)
}

// CrashNode 模拟节点崩溃：进程退出，停止响应；持久日志保留在磁盘。
func (c *Cluster) CrashNode(id string) error {
	// TODO(impl)
	return nil
}

// RestartNode 模拟节点恢复：从磁盘重载日志并重新加入分片。
func (c *Cluster) RestartNode(id string) error {
	// TODO(impl)
	return nil
}

// GetNode 返回节点（即使处于崩溃状态也返回句柄）。
func (c *Cluster) GetNode(id string) (*Node, error) {
	// TODO(impl)
	return nil, nil
}

// ShardLeader 返回某分片当前主副本 ID（无主返回 ""）。
func (c *Cluster) ShardLeader(shardID int) string {
	// TODO(impl)
	return ""
}

// WaitForLeader 轮询等待某分片在 timeout 内选出主；超时返回错误。
func (c *Cluster) WaitForLeader(shardID int, timeout time.Duration) (string, error) {
	// TODO(impl)
	return "", nil
}

// ShardReplicas 返回某分片的副本 ID 列表。
func (c *Cluster) ShardReplicas(shardID int) ([]string, error) {
	// TODO(impl)
	return nil, nil
}

// Network 暴露故障注入网络。
func (c *Cluster) Network() *Network {
	// TODO(impl)
	return nil
}

// RequestVote 实现 Transport：经 Network 投递投票 RPC。
func (c *Cluster) RequestVote(to string, req VoteRequest) (VoteResponse, error) {
	// TODO(impl)
	return VoteResponse{}, nil
}

// AppendEntries 实现 Transport：经 Network 投递复制 RPC。
func (c *Cluster) AppendEntries(to string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	// TODO(impl)
	return AppendEntriesResponse{}, nil
}

// Write 是面向客户端的写入口：定位主副本并提交。
func (c *Cluster) Write(req WriteRequest) (WriteResponse, error) {
	// TODO(impl)
	return WriteResponse{}, nil
}

// Read 是面向客户端的读入口；Mode 决定强一致或最终一致语义。
func (c *Cluster) Read(req ReadRequest) (ReadResponse, error) {
	// TODO(impl)
	return ReadResponse{}, nil
}

// Retry 以固定退避重试一次客户端操作，直到成功或 ctx 类条件不再满足。
func (c *Cluster) Retry(times int, backoff time.Duration, fn func() error) error {
	// TODO(impl)
	return nil
}
