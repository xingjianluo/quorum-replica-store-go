package store

import (
	"encoding/json"
	"hash/fnv"
	mathrand "math/rand"
	"os"
	"path/filepath"
	"sync"
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

// discardLogger 在未提供日志器时使用。
type discardLogger struct{}

func (discardLogger) Printf(string, ...any) {}

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
	ShardID      int
	Term         int64
	Leader       string
	PrevLogIndex int64
	PrevLogTerm  int64
	Entries      []Entry
	LeaderCommit int64
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
	DataDir            string // 持久日志目录；为空表示纯内存
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

const (
	submitTimeout = 3 * time.Second
	pollInterval  = 5 * time.Millisecond
)

// proposal 是主副本上等待多数派确认的一个写提案。
type proposal struct {
	entry    Entry
	ack      map[string]bool
	notifyCh chan proposalResult
}

type proposalResult struct {
	version int64
	err     error
}

// Node 是某分片的一个副本：持久日志 + 状态机 + Raft 复制状态。
type Node struct {
	cfg NodeConfig
	log Logger

	mu     sync.Mutex
	replMu sync.Mutex
	// 持久状态（崩溃后必须恢复）
	dlog        *DurableLog
	sm          *StateMachine
	currentTerm int64
	votedFor    string
	statePath   string

	// 易失状态
	role         Role
	leader       string
	commitIndex  int64
	lastApplied  int64
	electionDead time.Time
	lastHeartbeat time.Time
	down         bool
	stopped      bool
	rng          *mathrand.Rand
	stopCh       chan struct{}

	// 仅 leader 使用
	nextIndex  map[string]int64
	matchIndex map[string]int64
	pending    map[int64]*proposal // logIndex -> 等待确认的提案
}

// NewNode 构造一个副本并加载本地持久日志与已投过票的任期。
func NewNode(cfg NodeConfig) (*Node, error) {
	lg := cfg.Logger
	if lg == nil {
		lg = discardLogger{}
	}
	h := fnv.New64a()
	h.Write([]byte(cfg.ID))
	seed := int64(h.Sum64()) ^ time.Now().UnixNano()
	n := &Node{
		cfg:        cfg,
		log:        lg,
		role:       Follower,
		nextIndex:  make(map[string]int64),
		matchIndex: make(map[string]int64),
		pending:    make(map[int64]*proposal),
		rng:        mathrand.New(mathrand.NewSource(seed)),
	}
	logPath := ""
	if cfg.DataDir != "" {
		dir := filepath.Join(cfg.DataDir, cfg.ID)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, wrapError(CodeUnavailable, "mkdir node data dir", err)
		}
		logPath = filepath.Join(dir, "log.jsonl")
		n.statePath = filepath.Join(dir, "state.json")
	}
	dl, err := NewDurableLog(logPath)
	if err != nil {
		return nil, err
	}
	n.dlog = dl
	n.sm = NewStateMachine()

	// 恢复持久任期与投票，防止旧主重启后用旧任期重新当选（旧主不得覆盖新主已提交数据）。
	if err := n.loadPersistedState(); err != nil {
		return nil, err
	}

	// 重放日志重建已应用状态与幂等结果表。
	_, last := dl.Last()
	for _, e := range dl.Entries(1, last) {
		n.sm.Apply(e)
	}
	n.lastApplied = last
	n.commitIndex = last // 保守恢复：重启后等待 leader 的 commit 确认再推进
	n.resetElectionDeadlineLocked(time.Now())
	return n, nil
}

// persistedState 是需要随日志一起落盘的 Raft 硬状态。
type persistedState struct {
	CurrentTerm int64  `json:"current_term"`
	VotedFor    string `json:"voted_for"`
}

func (n *Node) loadPersistedState() error {
	if n.statePath == "" {
		return nil
	}
	b, err := os.ReadFile(n.statePath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return wrapError(CodeUnavailable, "read persisted state", err)
	}
	var ps persistedState
	if len(b) > 0 {
		if err := json.Unmarshal(b, &ps); err != nil {
			return wrapError(CodeUnavailable, "parse persisted state", err)
		}
	}
	n.currentTerm = ps.CurrentTerm
	n.votedFor = ps.VotedFor
	return nil
}

// persistStateLocked 把当前任期与投票落盘（调用方持锁）。
func (n *Node) persistStateLocked() error {
	if n.statePath == "" {
		return nil
	}
	ps := persistedState{CurrentTerm: n.currentTerm, VotedFor: n.votedFor}
	b, err := json.Marshal(&ps)
	if err != nil {
		return wrapError(CodeUnavailable, "marshal persisted state", err)
	}
	tmp := n.statePath + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return wrapError(CodeUnavailable, "write persisted state", err)
	}
	if err := os.Rename(tmp, n.statePath); err != nil {
		return wrapError(CodeUnavailable, "rename persisted state", err)
	}
	return nil
}

// State 返回节点状态快照。
func (n *Node) State() NodeState {
	n.mu.Lock()
	defer n.mu.Unlock()
	_, li := n.dlog.Last()
	return NodeState{
		ID: n.cfg.ID, ShardID: n.cfg.ShardID,
		Role: n.role, Term: n.currentTerm, Leader: n.leader,
		CommitIndex: n.commitIndex, LastIndex: li, VotedFor: n.votedFor,
		Peers: append([]string(nil), n.cfg.Peers...), Down: n.down,
	}
}

// LeaderHint 返回节点当前认知的主副本 ID（可能为空）。
func (n *Node) LeaderHint() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leader
}

// Term 返回节点当前任期。
func (n *Node) Term() int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm
}

// resetElectionDeadlineLocked 安排下一次随机选举超时（调用方持锁）。
func (n *Node) resetElectionDeadlineLocked(now time.Time) {
	lo := n.cfg.ElectionTimeoutMin
	hi := n.cfg.ElectionTimeoutMax
	if lo <= 0 {
		lo = 300 * time.Millisecond
	}
	if hi <= lo {
		hi = lo + 150*time.Millisecond
	}
	span := int64(hi - lo)
	d := lo
	if span > 0 {
		d += time.Duration(n.rng.Int63n(span))
	}
	n.electionDead = now.Add(d)
}

// isLogUpToDateLocked 判断候选人的日志是否至少与本节点一样新
// （Raft 规则：先比最后一条的任期，再比下标）。调用方持锁。
func (n *Node) isLogUpToDateLocked(candTerm, candIndex int64) bool {
	lastTerm, lastIndex := n.dlog.Last()
	if candTerm != lastTerm {
		return candTerm > lastTerm
	}
	return candIndex >= lastIndex
}

// stepDownLocked 在见到更高任期时退回 follower（旧主恢复后不会再以旧主身份接受写）。
func (n *Node) stepDownLocked(term int64, leader string) {
	n.role = Follower
	n.currentTerm = term
	n.leader = leader
	n.nextIndex = make(map[string]int64)
	n.matchIndex = make(map[string]int64)
	n.pending = make(map[int64]*proposal)
}

// HandleRequestVote 处理投票 RPC，返回中带上明确的拒绝/同意依据。
func (n *Node) HandleRequestVote(req VoteRequest) VoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.down {
		return VoteResponse{Term: n.currentTerm, Granted: false, Reason: "node is down"}
	}
	lastTerm, lastIndex := n.dlog.Last()

	if req.Term < n.currentTerm {
		return VoteResponse{Term: n.currentTerm, Granted: false,
			Reason: "reject vote: candidate term " + itoa64(req.Term) +
				" < current term " + itoa64(n.currentTerm)}
	}
	if req.Term > n.currentTerm {
		n.stepDownLocked(req.Term, "")
		if err := n.persistStateLocked(); err != nil {
			return VoteResponse{Term: n.currentTerm, Reason: "persist state: " + err.Error()}
		}
	}
	if n.role == Leader {
		// 同任期内不应出现第二个合法候选人；拒绝并说明。
		return VoteResponse{Term: n.currentTerm, Granted: false,
			Reason: "reject vote: already leader in term " + itoa64(n.currentTerm)}
	}
	if n.votedFor != "" && n.votedFor != req.Candidate {
		return VoteResponse{Term: n.currentTerm, Granted: false,
			Reason: "reject vote: already voted for " + n.votedFor +
				" in term " + itoa64(n.currentTerm)}
	}
	if !n.isLogUpToDateLocked(req.LastLogTerm, req.LastLogIndex) {
		return VoteResponse{Term: n.currentTerm, Granted: false,
			Reason: "reject vote: candidate log (t" + itoa64(req.LastLogTerm) +
				",i" + itoa64(req.LastLogIndex) + ") is behind local (t" +
				itoa64(lastTerm) + ",i" + itoa64(lastIndex) + ")"}
	}

	n.votedFor = req.Candidate
	if err := n.persistStateLocked(); err != nil {
		return VoteResponse{Term: n.currentTerm, Reason: "persist vote: " + err.Error()}
	}
	n.resetElectionDeadlineLocked(time.Now())
	n.log.Printf("[node %s shard %d] GRANT vote to %s in term %d (local log t%d/i%d)",
		n.cfg.ID, n.cfg.ShardID, req.Candidate, n.currentTerm, lastTerm, lastIndex)
	return VoteResponse{Term: n.currentTerm, Granted: true,
		Reason: "granted: log up-to-date and no prior vote in term " + itoa64(n.currentTerm)}
}

// HandleAppendEntries 处理复制/心跳 RPC。
func (n *Node) HandleAppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.down {
		return AppendEntriesResponse{Term: n.currentTerm, Reason: "node is down"}
	}

	if req.Term < n.currentTerm {
		return AppendEntriesResponse{Term: n.currentTerm, Success: false,
			Reason: "reject append: leader term " + itoa64(req.Term) +
				" < current term " + itoa64(n.currentTerm)}
	}
	if req.Term > n.currentTerm {
		n.stepDownLocked(req.Term, req.Leader)
		if err := n.persistStateLocked(); err != nil {
			return AppendEntriesResponse{Term: n.currentTerm, Reason: "persist state: " + err.Error()}
		}
	} else if n.role != Follower {
		// 见到同任期合法 leader -> 退回 follower。
		n.stepDownLocked(req.Term, req.Leader)
	}
	n.leader = req.Leader
	n.resetElectionDeadlineLocked(time.Now())

	// 一致性检查：prevLogIndex 处的日志任期必须匹配。
	_, lastIndex := n.dlog.Last()
	if req.PrevLogIndex > 0 {
		prev, ok := n.dlog.EntryAt(req.PrevLogIndex)
		if !ok {
			// 本地缺日志：快速回退提示。
			return AppendEntriesResponse{Term: n.currentTerm, Success: false,
				ConflictIndex: lastIndex + 1, ConflictTerm: 0,
				Reason: "conflict: missing prev entry at index " + itoa64(req.PrevLogIndex) +
					", local last index " + itoa64(lastIndex)}
		}
		if prev.Term != req.PrevLogTerm {
			// 任期冲突：返回冲突任期及其首条下标，供 leader 跳过整个冲突任期。
			ci := firstIndexOfTermLocked(n.dlog, prev.Term)
			return AppendEntriesResponse{Term: n.currentTerm, Success: false,
				ConflictIndex: ci, ConflictTerm: prev.Term,
				Reason: "conflict: prev term at index " + itoa64(req.PrevLogIndex) +
					" is " + itoa64(prev.Term) + ", want " + itoa64(req.PrevLogTerm)}
		}
	}

	// 归并新条目：在第一个不一致处截断本地后缀（绝不会截断已提交日志）。
	appendAt := req.PrevLogIndex
	for _, e := range req.Entries {
		appendAt++
		existing, ok := n.dlog.EntryAt(appendAt)
		if ok && existing.Term != e.Term {
			if err := n.dlog.Truncate(appendAt - 1); err != nil {
				return AppendEntriesResponse{Term: n.currentTerm, Reason: "truncate: " + err.Error()}
			}
		}
		existing, ok = n.dlog.EntryAt(appendAt)
		if !ok {
			if err := n.dlog.Append(e); err != nil {
				return AppendEntriesResponse{Term: n.currentTerm, Reason: "append: " + err.Error()}
			}
		}
	}

	// 推进已提交位置并应用到状态机。
	newCommit := req.LeaderCommit
	if last := req.PrevLogIndex + int64(len(req.Entries)); newCommit > last {
		newCommit = last
	}
	if newCommit > n.commitIndex {
		n.commitIndex = newCommit
		n.applyCommittedLocked()
	}
	return AppendEntriesResponse{Term: n.currentTerm, Success: true,
		Reason: "appended up to index " + itoa64(req.PrevLogIndex+int64(len(req.Entries))) +
			", commit " + itoa64(n.commitIndex)}
}

// firstIndexOfTermLocked 返回 term 的第一条日志下标（线性扫描，规模小）。
func firstIndexOfTermLocked(dl *DurableLog, term int64) int64 {
	_, last := dl.Last()
	entries := dl.Entries(1, last)
	for i := range entries {
		if entries[i].Term == term {
			return entries[i].Index
		}
	}
	return 1
}

// applyCommittedLocked 把 (lastApplied, commitIndex] 的日志按序应用到状态机，
// 并（在主上）唤醒对应提案。调用方持锁。
func (n *Node) applyCommittedLocked() {
	if n.commitIndex <= n.lastApplied {
		return
	}
	entries := n.dlog.Entries(n.lastApplied+1, n.commitIndex+1) // 半开上界 +1
	for _, e := range entries {
		res := n.sm.Apply(e)
		n.lastApplied = e.Index
		if p, ok := n.pending[e.Index]; ok {
			delete(n.pending, e.Index)
			out := proposalResult{version: res.Version}
			if !res.OK {
				out.err = newError(res.Code, "write rejected at apply for request "+e.RequestID)
			}
			select {
			case p.notifyCh <- out:
			default: // buffered channel 已有结果或接收方已放弃，绝不阻塞提交路径
			}
		}
	}
}

// replicaCount 返回分片副本总数（含自己）。
func (n *Node) replicaCount() int { return len(n.cfg.Peers) + 1 }

// becomeLeaderLocked 完成候选人 -> 主的状态转换并初始化复制视图。调用方持锁。
func (n *Node) becomeLeaderLocked() {
	n.role = Leader
	n.leader = n.cfg.ID
	_, last := n.dlog.Last()
	for _, p := range n.cfg.Peers {
		n.nextIndex[p] = last + 1
		n.matchIndex[p] = 0
	}
	n.lastHeartbeat = time.Time{} // 立即触发一轮心跳
	n.log.Printf("[node %s shard %d] BECAME LEADER term=%d lastIndex=%d replicas=%d majority=%d",
		n.cfg.ID, n.cfg.ShardID, n.currentTerm, last, n.replicaCount(), Majority(n.replicaCount()))
}

// RaftTick 由驱动循环周期调用：follower/candidate 检查选举超时，leader 发心跳。
func (n *Node) RaftTick(now time.Time) {
	n.mu.Lock()
	if n.down || n.stopped {
		n.mu.Unlock()
		return
	}
	if n.role == Leader {
		interval := n.cfg.ElectionTimeoutMin / 2
		if interval <= 0 {
			interval = 150 * time.Millisecond
		}
		due := now.Sub(n.lastHeartbeat) >= interval
		n.mu.Unlock()
		if due {
			n.replicateAll()
		}
		return
	}
	timeout := !now.Before(n.electionDead)
	n.mu.Unlock()
	if timeout {
		n.startElection()
	}
}

// startElection 发起一轮选举：自增任期、投自己、向所有可达对端拉票，
// 得多数票则当选；拒绝票与不可达都计入判定依据日志。
func (n *Node) startElection() {
	n.mu.Lock()
	if n.down || n.stopped {
		n.mu.Unlock()
		return
	}
	n.currentTerm++
	n.role = Candidate
	n.votedFor = n.cfg.ID
	n.leader = ""
	if err := n.persistStateLocked(); err != nil {
		n.log.Printf("[node %s shard %d] persist term failed: %v", n.cfg.ID, n.cfg.ShardID, err)
	}
	lastTerm, lastIndex := n.dlog.Last()
	term := n.currentTerm
	peers := append([]string(nil), n.cfg.Peers...)
	n.resetElectionDeadlineLocked(time.Now())
	n.mu.Unlock()

	n.log.Printf("[node %s shard %d] START ELECTION term=%d (log t%d/i%d), requesting votes from %d peers",
		n.cfg.ID, n.cfg.ShardID, term, lastTerm, lastIndex, len(peers))

	votes := 1 // 自己
	granted, refused, unreachable := []string{n.cfg.ID}, []string{}, []string{}
	req := VoteRequest{ShardID: n.cfg.ShardID, Term: term, Candidate: n.cfg.ID,
		LastLogIndex: lastIndex, LastLogTerm: lastTerm}
	for _, p := range peers {
		resp, err := n.cfg.Transport.RequestVote(p, req)
		if err != nil {
			unreachable = append(unreachable, p+"("+err.Error()+")")
			continue
		}
		if resp.Granted {
			votes++
			granted = append(granted, p)
		} else {
			refused = append(refused, p+"["+resp.Reason+"]")
		}
		n.mu.Lock()
		if resp.Term > term && resp.Term > n.currentTerm {
			n.stepDownLocked(resp.Term, "")
			_ = n.persistStateLocked()
			n.mu.Unlock()
			n.log.Printf("[node %s shard %d] abort election: saw higher term %d from %s",
				n.cfg.ID, n.cfg.ShardID, resp.Term, p)
			return
		}
		n.mu.Unlock()
	}

	need := Majority(n.replicaCount())
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.down || n.stopped || n.role != Candidate || n.currentTerm != term {
		n.log.Printf("[node %s shard %d] election term=%d superseded (role=%s term=%d)",
			n.cfg.ID, n.cfg.ShardID, term, n.role, n.currentTerm)
		return
	}
	n.log.Printf("[node %s shard %d] election tally term=%d votes=%d/%d granted=%v refused=%v unreachable=%v",
		n.cfg.ID, n.cfg.ShardID, term, votes, n.replicaCount(), granted, refused, unreachable)
	if votes >= need {
		n.becomeLeaderLocked()
	} else {
		n.log.Printf("[node %s shard %d] cannot win: votes=%d < majority=%d (granted=%v, unreachable=%v)",
			n.cfg.ID, n.cfg.ShardID, votes, need, granted, unreachable)
	}
}

// replicateMu 串行化复制轮次（心跳与提交触发可能来自不同 goroutine）。
// replicateAll 发送一轮 AppendEntries（也是主的心跳），处理冲突回退，
// 按多数派 matchIndex 推进 commitIndex，并返回同任期成功确认的副本数（含自己）。
func (n *Node) replicateAll() int {
	n.replMu.Lock()
	defer n.replMu.Unlock()

	type job struct {
		peer         string
		prevIndex    int64
		prevTerm     int64
		entries      []Entry
		leaderCommit int64
		term         int64
	}
	n.mu.Lock()
	if n.down || n.stopped || n.role != Leader {
		n.mu.Unlock()
		return 0
	}
	term := n.currentTerm
	jobs := make([]job, 0, len(n.cfg.Peers))
	for _, p := range n.cfg.Peers {
		ni := n.nextIndex[p]
		if ni < 1 {
			ni = 1
		}
		prevIndex := ni - 1
		var prevTerm int64
		if prevIndex > 0 {
			if e, ok := n.dlog.EntryAt(prevIndex); ok {
				prevTerm = e.Term
			}
		}
		_, last := n.dlog.Last()
		// 半开区间 [ni, last+1)，Entries 内部会把上界夹到日志末尾
		entries := n.dlog.Entries(ni, last+1)
		jobs = append(jobs, job{p, prevIndex, prevTerm, entries, n.commitIndex, term})
	}
	n.lastHeartbeat = time.Now()
	n.mu.Unlock()

	acks := 1 // 主自己持有一份完整日志
	for _, j := range jobs {
		req := AppendEntriesRequest{ShardID: n.cfg.ShardID, Term: j.term, Leader: n.cfg.ID,
			PrevLogIndex: j.prevIndex, PrevLogTerm: j.prevTerm,
			Entries: j.entries, LeaderCommit: j.leaderCommit}
		resp, err := n.cfg.Transport.AppendEntries(j.peer, req)
		if err != nil {
			continue // 对端不可达（崩溃/分区）：不计确认，下一轮心跳重试
		}
		n.mu.Lock()
		if n.down || n.stopped || n.role != Leader || n.currentTerm != term {
			n.mu.Unlock()
			continue
		}
		if resp.Term > term {
			n.stepDownLocked(resp.Term, "")
			_ = n.persistStateLocked()
			n.mu.Unlock()
			n.log.Printf("[node %s shard %d] lost leadership: %s reported higher term %d",
				n.cfg.ID, n.cfg.ShardID, j.peer, resp.Term)
			continue
		}
		if resp.Success {
			if len(j.entries) > 0 {
				// 只有实际携带了日志时才推进 follower 的进度视图；
				// 空心跳成功不得把 nextIndex/matchIndex 压回到 prevIndex。
				matched := j.prevIndex + int64(len(j.entries))
				if matched > n.matchIndex[j.peer] {
					n.matchIndex[j.peer] = matched
				}
				if matched+1 > n.nextIndex[j.peer] {
					n.nextIndex[j.peer] = matched + 1
				}
			}
			acks++
		} else {
			cf := resp.ConflictIndex
			if cf < 1 {
				cf = 1
			}
			n.nextIndex[j.peer] = cf
			n.log.Printf("[node %s shard %d] peer %s rejected append (t%d): %s; backoff nextIndex->%d",
				n.cfg.ID, n.cfg.ShardID, j.peer, term, resp.Reason, cf)
		}
		n.advanceCommitLocked(term)
		n.mu.Unlock()
	}
	return acks
}

// advanceCommitLocked 按多数派 matchIndex 推进 commitIndex。
// 遵循 Raft 约束：只能直接提交当前任期的条目；更早任期条目随其后被间接提交。
// 调用方持锁。
func (n *Node) advanceCommitLocked(term int64) {
	_, last := n.dlog.Last()
	need := Majority(n.replicaCount())
	for idx := n.commitIndex + 1; idx <= last; idx++ {
		e, ok := n.dlog.EntryAt(idx)
		if !ok || e.Term != term {
			continue // 不能直接提交旧任期条目
		}
		count := 1 // leader 自己
		for _, p := range n.cfg.Peers {
			if n.matchIndex[p] >= idx {
				count++
			}
		}
		if count >= need {
			n.commitIndex = idx
			n.applyCommittedLocked()
		}
	}
}

// SetDown 供 Cluster 在崩溃/恢复时调用（恢复不重置持久状态）。
func (n *Node) SetDown(down bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.down = down
	if down {
		n.role = Follower
		n.leader = ""
		n.pending = make(map[int64]*proposal)
		n.matchIndex = make(map[string]int64)
		n.nextIndex = make(map[string]int64)
		return
	}
	n.resetElectionDeadlineLocked(time.Now())
}

// countReachablePeersLocked 返回当前可达的对端数（不含自己）。调用方持锁。
func (n *Node) countReachablePeersLocked() int {
	c := 0
	for _, p := range n.cfg.Peers {
		if n.cfg.Net.Reachable(n.cfg.ID, p) {
			c++
		}
	}
	return c
}

// Submit 是客户端写路径：仅主副本接受。
// 流程：身份校验 -> 幂等快查（已提交过的请求直接返回首次结果）->
// 追加本地日志 -> 复制并等多数派 -> 应用并返回新版本。
// requestID 非空时，同一请求重复提交保证只生效一次。
func (n *Node) Submit(requestID string, op Op, key string, value []byte, expectedVersion int64) (int64, error) {
	n.mu.Lock()
	if n.down {
		n.mu.Unlock()
		return 0, newError(CodeUnavailable, "node "+n.cfg.ID+" is down")
	}
	if n.role != Leader {
		leader := n.leader
		term := n.currentTerm
		n.mu.Unlock()
		reason := "node " + n.cfg.ID + " is " + n.role.String() + " in term " + itoa64(term)
		if leader != "" {
			reason += ", current leader is " + leader
		} else {
			reason += ", no leader known yet"
		}
		return 0, newError(CodeNotLeader, reason)
	}

	// 幂等快查：同一请求标识已落定则直接返回首次结果，绝不再改数据。
	if requestID != "" {
		if r, ok := n.sm.Result(requestID); ok {
			n.mu.Unlock()
			if r.OK {
				return r.Version, nil
			}
			return r.Version, newError(r.Code, "duplicate request "+requestID+" returns first outcome")
		}
	}

	// 提交前多数派可达性预检：连多数派都凑不出来时立即拒绝，避免悬挂等待。
	reachable := n.countReachablePeersLocked() + 1
	need := Majority(n.replicaCount())
	if reachable < need {
		n.mu.Unlock()
		return 0, newError(CodeNoQuorum,
			"write rejected before append: reachable replicas "+itoa(reachable)+
				" < majority "+itoa(need)+" (shard replicas "+itoa(n.replicaCount())+")")
	}

	_, idx := n.dlog.Last()
	entry := Entry{
		Term: n.currentTerm, Index: idx + 1, Op: op, Key: key,
		Value: append([]byte(nil), value...), ExpectedVersion: expectedVersion,
		RequestID: requestID,
	}
	if err := n.dlog.Append(entry); err != nil {
		n.mu.Unlock()
		return 0, err
	}
	ch := make(chan proposalResult, 1)
	n.pending[entry.Index] = &proposal{entry: entry, notifyCh: ch,
		ack: map[string]bool{n.cfg.ID: true}}
	term := n.currentTerm
	n.mu.Unlock()

	// 立即推动复制，而不是等下一次心跳，以降低写入延迟。
	n.replicateAll()

	// 等待该条目被多数派提交并应用；applyCommittedLocked 会在提交时向 notifyCh 投递结果。
	// 同时周期性重发复制并检查领导权/多数派，以便主切换或分区时能尽快给出可区分结论。
	deadline := time.NewTimer(submitTimeout)
	defer deadline.Stop()
	tick := time.NewTicker(pollInterval)
	defer tick.Stop()
	for {
		select {
		case res := <-ch:
			if res.err != nil {
				return res.version, res.err
			}
			return res.version, nil
		case <-tick.C:
			n.mu.Lock()
			if n.down {
				n.mu.Unlock()
				return 0, newError(CodeUnavailable, "leader "+n.cfg.ID+" crashed during submit")
			}
			if n.role != Leader || n.currentTerm != term {
				n.mu.Unlock()
				return 0, newError(CodeNoShardLeader,
					"lost leadership in term "+itoa64(term)+" while awaiting commit; retry with new leader")
			}
			reachable = n.countReachablePeersLocked() + 1
			n.mu.Unlock()
			if reachable < need {
				return 0, newError(CodeNoQuorum,
					"write cannot reach quorum while awaiting commit: reachable "+
						itoa(reachable)+" < majority "+itoa(need))
			}
			n.replicateAll()
		case <-deadline.C:
			return 0, newError(CodeNoQuorum,
				"write timed out awaiting quorum commit (majority "+itoa(need)+")")
		}
	}
}

// LocalRead 直接读本地已应用状态：最终一致读，返回可能落后的值，绝不返回未应用数据。
func (n *Node) LocalRead(key string) (KVItem, error) {
	n.mu.Lock()
	down := n.down
	n.mu.Unlock()
	if down {
		return KVItem{}, newError(CodeUnavailable, "node "+n.cfg.ID+" is down")
	}
	return n.sm.Get(key)
}

// LeaderRead 是强一致读：先以一轮心跳确认本节点仍是主且与多数派连通，
// 再读本地已应用（=已多数派提交）状态。旧主或多数派被隔离时必须拒绝。
func (n *Node) LeaderRead(key string) (KVItem, error) {
	n.mu.Lock()
	if n.down {
		n.mu.Unlock()
		return KVItem{}, newError(CodeUnavailable, "node "+n.cfg.ID+" is down")
	}
	if n.role != Leader {
		leader := n.leader
		n.mu.Unlock()
		if leader == "" {
			return KVItem{}, newError(CodeNoShardLeader,
				"strong read rejected: no confirmed leader for shard (election in progress?)")
		}
		return KVItem{}, newError(CodeReadModeConflict,
			"strong read rejected: node "+n.cfg.ID+" is not leader (hint: "+leader+")")
	}
	term := n.currentTerm
	need := Majority(n.replicaCount())
	n.mu.Unlock()

	acks := n.replicateAll() // 领导权确认 + 提交传播
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.down || n.role != Leader || n.currentTerm != term {
		return KVItem{}, newError(CodeReadModeConflict,
			"strong read rejected: leadership lost during confirmation (old leader fence)")
	}
	if acks < need {
		return KVItem{}, newError(CodeNoQuorum,
			"strong read rejected: leadership confirmed by only "+itoa(acks)+
				" replicas, need majority "+itoa(need)+"; partition suspected")
	}
	return n.sm.Get(key)
}

// Start 启动本节点的驱动 goroutine（周期触发选举检查或心跳）。
func (n *Node) Start(tick time.Duration) {
	n.mu.Lock()
	n.stopped = false
	if n.stopCh == nil {
		n.stopCh = make(chan struct{})
	}
	ch := n.stopCh
	n.mu.Unlock()
	if tick <= 0 {
		tick = 20 * time.Millisecond
	}
	go func() {
		t := time.NewTicker(tick)
		defer t.Stop()
		for {
			select {
			case <-ch:
				return
			case now := <-t.C:
				n.RaftTick(now)
			}
		}
	}()
}

// Shutdown 停止节点驱动（不删除持久日志，不影响崩溃恢复语义）。
func (n *Node) Shutdown() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	n.stopped = true
	ch := n.stopCh
	n.mu.Unlock()
	if ch != nil {
		close(ch)
		n.stopCh = nil
	}
	_ = n.dlog.Close()
}

