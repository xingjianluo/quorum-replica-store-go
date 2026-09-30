package store

import (
	"time"
)

// Role 是节点在 Raft 状态机中的角色。
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "follower"
	}
}

// Logger 是最小日志接口（判定依据日志通过它输出，便于测试解释拒绝/选主原因）。
type Logger interface {
	Printf(format string, args ...any)
}

// Transport 是节点间 RPC 通道，由 Cluster 实现：负责寻址并统一经过 Network 故障注入点。
type Transport interface {
	RequestVote(to string, req VoteRequest) (VoteResponse, error)
	AppendEntries(to string, req AppendEntriesRequest) (AppendEntriesResponse, error)
}

// VoteRequest 是候选人发起投票的参数。
type VoteRequest struct {
	ShardID      int
	Term         int64
	Candidate    string
	LastLogIndex int64
	LastLogTerm  int64
}

// VoteResponse 携带投票结果与拒绝依据（Reason 用于日志可解释性）。
type VoteResponse struct {
	Term    int64
	Granted bool
	Reason  string
}

// AppendEntriesRequest 是主副本的复制/心跳 RPC。
type AppendEntriesRequest struct {
	ShardID       int
	Term          int64
	Leader        string
	PrevLogIndex  int64
	PrevLogTerm   int64
	Entries       []Entry
	LeaderCommit  int64
}

// AppendEntriesResponse 返回复制结果；冲突时返回 ConflictIndex/Term 供主回退 nextIndex。
type AppendEntriesResponse struct {
	Term          int64
	Success       bool
	ConflictIndex int64
	ConflictTerm  int64
	Reason        string
}

// NodeConfig 是构造副本节点所需的配置。
type NodeConfig struct {
	ID                 string
	ShardID            int
	Peers              []string
	DataDir            string // 持久日志目录；为空表示纯内存（崩溃即丢盘，仅特殊测试用）
	Transport          Transport
	Net                *Network
	Logger             Logger
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
}

// NodeState 是节点状态的只读快照（测试与可解释性断言用）。
type NodeState struct {
	ID          string
	ShardID     int
	Role        Role
	Term        int64
	Leader      string
	CommitIndex int64
	LastIndex   int64
	VotedFor    string
	Peers       []string
	Down        bool
}

// Node 是某分片的一个副本：持久日志 + 状态机 + Raft 复制状态。
type Node struct {
	cfg NodeConfig

	// TODO(impl): 持久日志与状态机
	// TODO(impl): currentTerm/votedFor/role/commitIndex 等易失状态
	// TODO(impl): leader 视图 nextIndex/matchIndex
	// TODO(impl): 选举定时器与心跳
}

// NewNode 构造一个副本并加载本地持久日志（若存在）。
func NewNode(cfg NodeConfig) (*Node, error) {
	// TODO(impl)
	return &Node{cfg: cfg}, nil
}

// Start 启动节点的选举与心跳驱动。
func (n *Node) Start() {
	// TODO(impl)
}

// Shutdown 停止节点驱动（不删除持久日志）。
func (n *Node) Shutdown() {
	// TODO(impl)
}

// RaftTick 由驱动循环周期调用：推进选举超时或心跳。
func (n *Node) RaftTick(now time.Time) {
	// TODO(impl)
}

// Submit 是客户端写路径：仅主副本接受，返回提交后的新版本号。
// requestID 非空时保证同一请求重复提交只生效一次。
func (n *Node) Submit(requestID string, op Op, key string, value []byte, expectedVersion int64) (int64, error) {
	// TODO(impl)
	return 0, nil
}

// LocalRead 直接读本地已应用状态（最终一致读，可能落后）。
func (n *Node) LocalRead(key string) (KVItem, error) {
	// TODO(impl)
	return KVItem{}, nil
}

// LeaderRead 在确认当前仍为主（向多数派发出一次心跳）后读本地状态（强一致读）。
func (n *Node) LeaderRead(key string) (KVItem, error) {
	// TODO(impl)
	return KVItem{}, nil
}

// HandleRequestVote 处理投票 RPC。
func (n *Node) HandleRequestVote(req VoteRequest) VoteResponse {
	// TODO(impl)
	return VoteResponse{}
}

// HandleAppendEntries 处理复制/心跳 RPC。
func (n *Node) HandleAppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	// TODO(impl)
	return AppendEntriesResponse{}
}

// State 返回节点状态快照。
func (n *Node) State() NodeState {
	// TODO(impl)
	return NodeState{ID: n.cfg.ID, ShardID: n.cfg.ShardID, Peers: append([]string(nil), n.cfg.Peers...)}
}

// LeaderHint 返回节点当前认知的主副本 ID（可能为空）。
func (n *Node) LeaderHint() string {
	// TODO(impl)
	return ""
}

// Term 返回节点当前任期。
func (n *Node) Term() int64 {
	// TODO(impl)
	return 0
}
