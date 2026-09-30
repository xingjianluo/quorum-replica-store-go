package store

import "sync"

// Network 是本模拟器的"网络层"：所有节点间 RPC 都必须经过它。
// 故障注入（节点崩溃、网络分区/隔离、恢复）全部在这里可控地发生，
// 因而测试场景完全离线、确定性、可复现。
type Network struct {
	mu sync.Mutex
	// down 表示节点已崩溃（不响应任何消息），恢复后日志从磁盘重载。
	down map[string]bool
	// links 记录被切断的双向链路集合 "a|b"（a<b 归一化）。
	links map[string]bool
}

// NewNetwork 创建一个无故障的全通网络。
func NewNetwork() *Network {
	return &Network{
		down:  make(map[string]bool),
		links: make(map[string]bool),
	}
}

// linkKey 把无序的节点对归一化成稳定的键。
func linkKey(a, b string) string {
	if a <= b {
		return a + "|" + b
	}
	return b + "|" + a
}

// SetNodeDown 设置节点的崩溃/恢复状态。
func (n *Network) SetNodeDown(id string, down bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.down[id] = down
}

// IsDown 报告节点是否处于崩溃状态。
func (n *Network) IsDown(id string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.down[id]
}

// CutLink 切断 a 与 b 之间的双向链路（分区）。
func (n *Network) CutLink(a, b string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.links[linkKey(a, b)] = true
}

// HealLink 恢复 a 与 b 之间的双向链路。
func (n *Network) HealLink(a, b string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.links, linkKey(a, b))
}

// Isolate 切断 id 与 others 中每个节点的链路（其余节点间保持互通）。
func (n *Network) Isolate(id string, others []string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, o := range others {
		if o == id {
			continue
		}
		n.links[linkKey(id, o)] = true
	}
}

// HealAll 恢复所有被切断的链路（不改变崩溃状态）。
func (n *Network) HealAll() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.links = make(map[string]bool)
}

// Reachable 判定从 a 到 b 当前是否可通信：双方都未崩溃且链路未被切断。
// 节点到自身恒为可达（未崩溃时）。
func (n *Network) Reachable(a, b string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.down[a] || n.down[b] {
		return false
	}
	if a == b {
		return true
	}
	return !n.links[linkKey(a, b)]
}
