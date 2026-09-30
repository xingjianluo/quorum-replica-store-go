package quorum

import (
	"context"
	"slices"
	"strings"
)

// Store routes key requests to the owning shard.
type Store struct {
	specs []ShardSpec
	sets  map[string]*ReplicaSet
	order []string
}

// NewStore constructs and validates a range-sharded store.
func NewStore(specs []ShardSpec) (*Store, error) {
	if len(specs) == 0 {
		return nil, newError(KindInvalidConfig, "new-store", "", "at least one shard is required")
	}
	ordered := make([]ShardSpec, len(specs))
	for i, spec := range specs {
		ordered[i] = ShardSpec{
			ID:         spec.ID,
			Range:      spec.Range,
			ReplicaIDs: append([]string(nil), spec.ReplicaIDs...),
		}
	}
	slices.SortFunc(ordered, func(a, b ShardSpec) int {
		if a.Range.Start < b.Range.Start {
			return -1
		}
		if a.Range.Start > b.Range.Start {
			return 1
		}
		return 0
	})
	if ordered[0].Range.Start != "" {
		return nil, newError(KindInvalidConfig, "new-store", ordered[0].ID,
			"first shard must start at the empty minimum key")
	}
	if ordered[len(ordered)-1].Range.End != "" {
		return nil, newError(KindInvalidConfig, "new-store", ordered[len(ordered)-1].ID,
			"last shard must end at the empty maximum key")
	}
	seen := make(map[string]bool)
	for i, spec := range ordered {
		if strings.TrimSpace(spec.ID) == "" {
			return nil, newError(KindInvalidConfig, "new-store", "", "shard id is empty")
		}
		if seen[spec.ID] {
			return nil, newError(KindInvalidConfig, "new-store", spec.ID, "duplicate shard id")
		}
		seen[spec.ID] = true
		if i > 0 && ordered[i-1].Range.End != spec.Range.Start {
			return nil, newError(KindInvalidConfig, "new-store", spec.ID,
				"shard range must continue exactly at the previous shard end")
		}
	}

	sets := make(map[string]*ReplicaSet, len(ordered))
	order := make([]string, 0, len(ordered))
	for _, spec := range ordered {
		set, err := NewReplicaSet(spec)
		if err != nil {
			return nil, err
		}
		sets[spec.ID] = set
		order = append(order, spec.ID)
	}
	return &Store{specs: ordered, sets: sets, order: order}, nil
}

// SetLogger assigns the same diagnostics logger to every shard.
func (s *Store) SetLogger(logger Logger) {
	for _, id := range s.order {
		s.sets[id].SetLogger(logger)
	}
}

// Specs returns the validated shard specifications in range order.
func (s *Store) Specs() []ShardSpec {
	out := make([]ShardSpec, len(s.specs))
	for i, spec := range s.specs {
		out[i] = ShardSpec{
			ID:         spec.ID,
			Range:      spec.Range,
			ReplicaIDs: append([]string(nil), spec.ReplicaIDs...),
		}
	}
	return out
}

// Shard returns the named replica set.
func (s *Store) Shard(id string) (*ReplicaSet, error) {
	set, ok := s.sets[id]
	if !ok {
		return nil, newError(KindNoShard, "shard", id, "unknown shard")
	}
	return set, nil
}

// ShardFor returns the shard that owns the supplied key.
func (s *Store) ShardFor(key string) (*ReplicaSet, error) {
	if key == "" {
		return nil, newError(KindInvalidArgument, "route", "", "key is empty")
	}
	for _, spec := range s.specs {
		if spec.Range.Contains(key) {
			return s.sets[spec.ID], nil
		}
	}
	return nil, newError(KindKeyOutOfRange, "route", "", "no shard range contains key "+key)
}

func (s *Store) routeLocked(reqShardID, key string) (*ReplicaSet, error) {
	if reqShardID != "" {
		set, err := s.Shard(reqShardID)
		if err != nil {
			return nil, err
		}
		if !set.spec.Range.Contains(key) {
			return nil, newError(KindKeyOutOfRange, "route", reqShardID,
				"key "+key+" is not owned by the addressed shard")
		}
		return set, nil
	}
	return s.ShardFor(key)
}

// Write routes an idempotent write to its owning shard.
func (s *Store) Write(ctx context.Context, req WriteRequest) (*WriteResponse, error) {
	set, err := s.routeLocked(req.ShardID, req.Key)
	if err != nil {
		return nil, err
	}
	return set.Write(ctx, req)
}

// Read routes a strong or eventual read to its owning shard.
func (s *Store) Read(ctx context.Context, req ReadRequest) (*ReadResponse, error) {
	set, err := s.routeLocked(req.ShardID, req.Key)
	if err != nil {
		return nil, err
	}
	return set.Read(ctx, req)
}

// CatchUp runs recovery for every shard. Shards without a majority fail.
func (s *Store) CatchUp() error {
	for _, id := range s.order {
		if err := s.sets[id].CatchUp(); err != nil {
			return err
		}
	}
	return nil
}
