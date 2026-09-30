package store

import "sort"

// Range 是一个左闭右开 [Start, End) 的键区间；End 为空表示无上界。
// 各分片区间必须互不相交并按序覆盖全部已配置的键空间。
type Range struct {
	Start string
	End   string // "" 表示 +inf
}

// contains 判断 key 是否落在本区间内。
func (r Range) contains(key string) bool {
	if key < r.Start {
		return false
	}
	if r.End != "" && key >= r.End {
		return false
	}
	return true
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
	byID   map[int]int   // 分片ID -> 在 shards 中的下标
}

// NewRouter 校验并构造路由器：区间必须非空、互不相交、按序邻接（首片可从 "" 开始）。
func NewRouter(shards []ShardConfig) (*Router, error) {
	if len(shards) == 0 {
		return nil, newError(CodeShardUnavailable, "router requires at least one shard")
	}
	out := make([]ShardConfig, len(shards))
	copy(out, shards)
	sort.Slice(out, func(i, j int) bool { return out[i].Range.Start < out[j].Range.Start })

	byID := make(map[int]int, len(out))
	for i, s := range out {
		if s.ID < 0 {
			return nil, newError(CodeShardUnavailable, "shard id must be non-negative")
		}
		if _, dup := byID[s.ID]; dup {
			return nil, newError(CodeShardUnavailable, "duplicate shard id")
		}
		byID[s.ID] = i
		if len(s.Replicas) == 0 {
			return nil, newError(CodeReplicasUnavailable,
				"shard "+itoa(s.ID)+" has zero replicas")
		}
		if len(s.Replicas)%2 == 0 {
			// 偶数副本不致命但会降低容错性价比；这里明确要求奇数副本以便多数派语义清晰。
			return nil, newError(CodeReplicasUnavailable,
				"shard "+itoa(s.ID)+" requires an odd replica count, got "+itoa(len(s.Replicas)))
		}
		// 检查相邻区间：必须左闭右开地首尾相接，不允许重叠或空洞。
		if i > 0 {
			prev := out[i-1].Range
			if prev.End == "" {
				return nil, newError(CodeShardUnavailable, "only the last shard may have an open end")
			}
			if prev.End != s.Range.Start {
				return nil, newError(CodeShardUnavailable,
					"non-contiguous shard ranges between "+prev.Start+".."+prev.End+" and "+s.Range.Start)
			}
		}
	}
	return &Router{shards: out, byID: byID}, nil
}

// Majority 返回 n 个副本所需的多数派票数（n/2 + 1）。
// n<=0 时返回 1（调用方应在更早处拒绝空副本集）。
func Majority(n int) int {
	if n <= 0 {
		return 1
	}
	return n/2 + 1
}

// Route 返回键所属分片；不属于任何分片时返回 CodeKeyOutOfRange。
func (r *Router) Route(key string) (ShardConfig, error) {
	for _, s := range r.shards {
		if s.Range.contains(key) {
			return s, nil
		}
	}
	return ShardConfig{}, newError(CodeKeyOutOfRange,
		"key "+quote(key)+" does not belong to any configured shard range")
}

// Shard 按 ID 查找分片配置。
func (r *Router) Shard(id int) (ShardConfig, bool) {
	idx, ok := r.byID[id]
	if !ok {
		return ShardConfig{}, false
	}
	return r.shards[idx], true
}

// Shards 返回全部分片配置（有序）。
func (r *Router) Shards() []ShardConfig {
	out := make([]ShardConfig, len(r.shards))
	copy(out, r.shards)
	return out
}

func itoa(i int) string {
	// 避免在基础文件里引入 strconv 的专用小工具。
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

func quote(s string) string { return "\"" + s + "\"" }
