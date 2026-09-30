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
	// TODO(impl)
	return &Network{}
}

func linkKey(a, b string) string {
	// TODO(impl)
	return ""
}

// SetNodeDown 设置节点的崩溃/恢复状态。
func (n *Network) SetNodeDown(id string, down bool) {
	// TODO(impl)
}

// IsDown 报告节点是否处于崩溃状态。
func (n *Network) IsDown(id string) bool {
	// TODO(impl)
	return false
}

// CutLink 切断 a 与 b 之间的双向链路（分区）。
func (n *Network) CutLink(a, b string) {
	// TODO(impl)
}

// HealLink 恢复 a 与 b 之间的双向链路。
func (n *Network) HealLink(a, b string) {
	// TODO(impl)
}

// Isolate 切断 id 与其他所有节点的链路（其余节点间保持互通）。
func (n *Network) Isolate(id string, others []string) {
	// TODO(impl)
}

// HealAll 恢复所有被切断的链路（不改变崩溃状态）。
func (n *Network) HealAll() {
	// TODO(impl)
}

// Reachable 判定从 a 到 b 当前是否可通信（双方未崩溃且链路未切断）。
func (n *Network) Reachable(a, b string) bool {
	// TODO(impl)
	return false
}
