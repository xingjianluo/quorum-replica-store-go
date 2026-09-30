package store

import "sort"

// Range 是一个左闭右开 [Start, End) 的键区间；End 为空表示无上界。
// 各分片区间必须互不相交并按序覆盖全部已配置的键空间。
type Range struct {
	Start string
	End   string // "" 表示 +inf
}

// ShardConfig 描述一个分片：ID、负责的键区间以及副本节点 ID 列表。
type ShardConfig struct {
	ID       int
	Range    Range
	Replicas []string // 副本集合，写/读多数派均按此集合大小计算
}

// Router 负责键 -> 分片的路由。
type Router struct {
	shards []ShardConfig // 已按 Range.Start 排序
}

// NewRouter 校验并构造路由器：区间必须非空、互不相交、按序邻接（首片可从 "" 开始）。
func NewRouter(shards []ShardConfig) (*Router, error) {
	// TODO(impl)
	_ = sort.Strings
	return &Router{}, nil
}

// Majority 返回 n 个副本所需的多数派票数（n/2 + 1）。
func Majority(n int) int {
	// TODO(impl)
	return 0
}

// Route 返回键所属分片；不属于任何分片时返回 CodeKeyOutOfRange。
func (r *Router) Route(key string) (ShardConfig, error) {
	// TODO(impl)
	return ShardConfig{}, nil
}

// Shard 按 ID 查找分片配置。
func (r *Router) Shard(id int) (ShardConfig, bool) {
	// TODO(impl)
	return ShardConfig{}, false
}

// Shards 返回全部分片配置（有序）。
func (r *Router) Shards() []ShardConfig {
	// TODO(impl)
	return nil
}
