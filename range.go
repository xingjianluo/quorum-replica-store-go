package quorum

// ShardRange is a half-open lexicographic interval [Start, End).
// Empty Start is the minimum key and empty End is the maximum key.
type ShardRange struct {
	Start string
	End   string
}

// Contains reports whether key belongs to the half-open range.
func (r ShardRange) Contains(key string) bool {
	if key < r.Start {
		return false
	}
	return r.End == "" || key < r.End
}

// ShardSpec declares one shard's interval and replica set.
type ShardSpec struct {
	ID         string
	Range      ShardRange
	ReplicaIDs []string
}
