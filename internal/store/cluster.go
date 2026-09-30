package store

import (
	"sync"
	"time"
)

// 默认选举超时：足够大以避免抖动，又足够小让本地测试快速观察切换。
const (
	defaultElectionMin = 200 * time.Millisecond
	defaultElectionMax = 600 * time.Millisecond
	defaultTick        = 20 * time.Millisecond
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
	cfg     map[string]NodeConfig
	logger  Logger
	dataDir string
	tick    time.Duration
	running bool
}

// NewCluster 创建集群。dataDir 非空时节点持久日志写入 dataDir/<node-id>/。
func NewCluster(router *Router, net *Network, dataDir string, logger Logger) *Cluster {
	if logger == nil {
		logger = discardLogger{}
	}
	if net == nil {
		net = NewNetwork()
	}
	return &Cluster{
		router:  router,
		net:     net,
		nodes:   make(map[string]*Node),
		owner:   make(map[string]int),
		cfg:     make(map[string]NodeConfig),
		logger:  logger,
		dataDir: dataDir,
		tick:    defaultTick,
	}
}

// AddReplica 为某分片构造并登记一个副本节点（构造后需 StartAll 才参与协议）。
func (c *Cluster) AddReplica(shardID int, id string) error {
	sc, ok := c.router.Shard(shardID)
	if !ok {
		return newError(CodeShardUnavailable, "no such shard "+itoa(shardID))
	}
	if _, exists := c.nodes[id]; exists {
		return newError(CodeUnavailable, "duplicate replica id "+id)
	}
	peers := make([]string, 0, len(sc.Replicas)-1)
	for _, r := range sc.Replicas {
		if r != id {
			peers = append(peers, r)
		}
	}
	cfg := NodeConfig{
		ID: id, ShardID: shardID, Peers: peers,
		DataDir: c.dataDir, Transport: c, Net: c.net, Logger: c.logger,
		ElectionTimeoutMin: defaultElectionMin, ElectionTimeoutMax: defaultElectionMax,
	}
	n, err := NewNode(cfg)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.nodes[id] = n
	c.owner[id] = shardID
	c.cfg[id] = cfg
	c.mu.Unlock()
	return nil
}

// StartAll 启动全部节点的驱动循环。
func (c *Cluster) StartAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.running = true
	for _, n := range c.nodes {
		n.Start(c.tick)
	}
}

// ShutdownAll 停止全部节点驱动。
func (c *Cluster) ShutdownAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.running = false
	for _, n := range c.nodes {
		n.Shutdown()
	}
}

// CrashNode 模拟节点崩溃：停止驱动、从网络摘除；持久日志保留在磁盘。
func (c *Cluster) CrashNode(id string) error {
	c.mu.RLock()
	n, ok := c.nodes[id]
	c.mu.RUnlock()
	if !ok {
		return newError(CodeUnavailable, "no such node "+id)
	}
	c.logger.Printf("[cluster] CRASH node %s (shard %d): process stopped, durable log retained",
		id, c.owner[id])
	n.SetDown(true)
	c.net.SetNodeDown(id, true)
	n.Shutdown()
	return nil
}

// RestartNode 模拟节点恢复：重新构造进程、从磁盘重载日志与持久任期后重新加入。
// 长时间离线者日志落后没关系，重新连通后会由主通过 AppendEntries 追赶收敛。
func (c *Cluster) RestartNode(id string) error {
	c.mu.RLock()
	cfg, ok := c.cfg[id]
	c.mu.RUnlock()
	if !ok {
		return newError(CodeUnavailable, "no such node "+id)
	}
	n, err := NewNode(cfg)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.nodes[id] = n
	c.mu.Unlock()
	n.SetDown(false)
	c.net.SetNodeDown(id, false)
	if c.running {
		n.Start(c.tick)
	}
	c.logger.Printf("[cluster] RESTART node %s (shard %d): reloaded term=%d lastIndex=%d",
		id, cfg.ShardID, n.Term(), n.State().LastIndex)
	return nil
}

// GetNode 返回节点句柄（崩溃后仍可能返回旧句柄）。
func (c *Cluster) GetNode(id string) (*Node, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	n, ok := c.nodes[id]
	if !ok {
		return nil, newError(CodeUnavailable, "no such node "+id)
	}
	return n, nil
}

// shardNodeIDs 返回某分片的全部节点 ID。
func (c *Cluster) shardNodeIDs(shardID int) []string {
	sc, ok := c.router.Shard(shardID)
	if !ok {
		return nil
	}
	return append([]string(nil), sc.Replicas...)
}

// ShardLeader 返回某分片当前主副本 ID。
// 若出现分区导致多个节点自认主，取最高任期者（被隔离旧主任期更低，不会被选中）。
func (c *Cluster) ShardLeader(shardID int) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var leader string
	var leaderTerm int64 = -1
	for _, id := range c.shardNodeIDs(shardID) {
		n, ok := c.nodes[id]
		if !ok || c.net.IsDown(id) {
			continue
		}
		st := n.State()
		if st.Role == Leader && st.Term > leaderTerm {
			leaderTerm = st.Term
			leader = id
		}
	}
	return leader
}

// WaitForLeader 轮询等待某分片在 timeout 内选出主。
func (c *Cluster) WaitForLeader(shardID int, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if l := c.ShardLeader(shardID); l != "" {
			return l, nil
		}
		time.Sleep(c.tick)
	}
	return "", newError(CodeNoShardLeader,
		"no leader elected for shard "+itoa(shardID)+" within "+timeout.String()+
			": reachable replicas cannot form a majority")
}

// ShardReplicas 返回某分片的副本 ID 列表。
func (c *Cluster) ShardReplicas(shardID int) ([]string, error) {
	if _, ok := c.router.Shard(shardID); !ok {
		return nil, newError(CodeShardUnavailable, "no such shard "+itoa(shardID))
	}
	return c.shardNodeIDs(shardID), nil
}

// Network 暴露故障注入网络。
func (c *Cluster) Network() *Network { return c.net }

// RequestVote 实现 Transport：统一经过 Network 故障注入点。
func (c *Cluster) RequestVote(to string, req VoteRequest) (VoteResponse, error) {
	c.mu.RLock()
	n, ok := c.nodes[to]
	c.mu.RUnlock()
	if !ok || !c.net.Reachable(req.Candidate, to) {
		return VoteResponse{}, newError(CodeUnavailable,
			"vote RPC "+req.Candidate+" -> "+to+" undeliverable (crash or partition)")
	}
	return n.HandleRequestVote(req), nil
}

// AppendEntries 实现 Transport：统一经过 Network 故障注入点。
func (c *Cluster) AppendEntries(to string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	c.mu.RLock()
	n, ok := c.nodes[to]
	c.mu.RUnlock()
	if !ok || !c.net.Reachable(req.Leader, to) {
		return AppendEntriesResponse{}, newError(CodeUnavailable,
			"append RPC "+req.Leader+" -> "+to+" undeliverable (crash or partition)")
	}
	return n.HandleAppendEntries(req), nil
}

// validateShardKey 校验分片存在且键确实属于该分片（跨分片写要明确拒绝）。
func (c *Cluster) validateShardKey(shardID int, key string) (ShardConfig, error) {
	sc, ok := c.router.Shard(shardID)
	if !ok {
		return ShardConfig{}, newError(CodeShardUnavailable, "no such shard "+itoa(shardID))
	}
	owner, err := c.router.Route(key)
	if err != nil {
		return ShardConfig{}, err
	}
	if owner.ID != shardID {
		return ShardConfig{}, newError(CodeKeyOutOfRange,
			"key "+quote(key)+" belongs to shard "+itoa(owner.ID)+", not shard "+itoa(shardID))
	}
	return sc, nil
}

// Write 是面向客户端的写入口：校验路由 -> 定位主 -> 提交多数派。
// 主失效/多数派不足等情况以可区分原因码返回，由 Client 决定是否重试。
func (c *Cluster) Write(req WriteRequest) (WriteResponse, error) {
	if _, err := c.validateShardKey(req.ShardID, req.Key); err != nil {
		return WriteResponse{}, err
	}
	leader := c.ShardLeader(req.ShardID)
	if leader == "" {
		sc, _ := c.router.Shard(req.ShardID)
		alive := 0
		for _, id := range sc.Replicas {
			if !c.net.IsDown(id) {
				alive++
			}
		}
		return WriteResponse{}, newError(CodeNoShardLeader,
			"no leader for shard "+itoa(req.ShardID)+": "+itoa(alive)+"/"+
				itoa(len(sc.Replicas))+" replicas alive, majority is "+
				itoa(Majority(len(sc.Replicas))))
	}
	n, _ := c.GetNode(leader)
	version, err := n.Submit(req.RequestID, req.Op, req.Key, req.Value, req.ExpectedVersion)
	if err != nil {
		return WriteResponse{}, err
	}
	return WriteResponse{Version: version, Leader: leader, FromRequestID: req.RequestID}, nil
}

// Read 是面向客户端的读入口。
//   - ReadStrong：只走主副本，主缺失或无法确认多数派时拒绝（ReadModeConflict/NoQuorum）。
//   - ReadEventual：任选一个未崩溃副本读本地已应用状态（可能落后），全部崩溃才拒绝。
func (c *Cluster) Read(req ReadRequest) (ReadResponse, error) {
	if _, err := c.validateShardKey(req.ShardID, req.Key); err != nil {
		return ReadResponse{}, err
	}
	if req.Mode == ReadStrong {
		leader := c.ShardLeader(req.ShardID)
		if leader == "" {
			return ReadResponse{}, newError(CodeNoShardLeader,
				"strong read: no confirmed leader for shard "+itoa(req.ShardID))
		}
		n, _ := c.GetNode(leader)
		item, err := n.LeaderRead(req.Key)
		if err != nil {
			return ReadResponse{}, err
		}
		return ReadResponse{Item: item, From: leader, Leader: leader, Stale: false}, nil
	}

	// 最终一致：优先主，其次任一存活副本（即使处于分区也可以读本地）。
	ids := c.shardNodeIDs(req.ShardID)
	var live []string
	if l := c.ShardLeader(req.ShardID); l != "" {
		live = append(live, l)
	}
	for _, id := range ids {
		if id == c.ShardLeader(req.ShardID) || c.net.IsDown(id) {
			continue
		}
		live = append(live, id)
	}
	if len(live) == 0 {
		return ReadResponse{}, newError(CodeReplicasUnavailable,
			"eventual read: no alive replica for shard "+itoa(req.ShardID))
	}
	var lastErr error
	for _, id := range live {
		n, _ := c.GetNode(id)
		item, err := n.LocalRead(req.Key)
		if err == nil {
			return ReadResponse{Item: item, From: id, Leader: c.ShardLeader(req.ShardID), Stale: id != c.ShardLeader(req.ShardID)}, nil
		}
		lastErr = err
	}
	return ReadResponse{}, lastErr
}

// Retry 以固定退避重试 fn：成功即返回；错误原因码在 fatalCodes 中时不重试。
func (c *Cluster) Retry(times int, backoff time.Duration, fatalCodes map[string]bool, fn func() error) error {
	var err error
	for i := 0; i < times; i++ {
		if err = fn(); err == nil {
			return nil
		}
		if fatalCodes[ErrorCode(err)] {
			return err
		}
		time.Sleep(backoff)
	}
	return err
}
