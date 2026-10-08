package quorum

import (
	"context"
	"slices"
	"strings"
	"sync"
	"time"
)

// Store routes key requests to the owning shard.
type Store struct {
	mu    sync.RWMutex
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

	sets, order, err := buildReplicaSets(ordered, nil)
	if err != nil {
		return nil, err
	}
	return &Store{specs: ordered, sets: sets, order: order}, nil
}

// storeConfig 控制后台节拍、自动接管和本地持久化目录。
type storeConfig struct {
	dataDir           string
	heartbeatInterval time.Duration
	electionTimeout   time.Duration
	autoFailover      bool
}

// StoreOption 构造期配置 Store 的后台行为。
type StoreOption func(*storeConfig)

// WithDataDirectory 开启本地持久化：提交多数派落盘后才对外确认，
// RestartStore 从同一目录重建整套实例。
func WithDataDirectory(dir string) StoreOption {
	return func(c *storeConfig) { c.dataDir = dir }
}

// WithFailoverTiming 覆盖心跳间隔、选举超时与自动接管开关。
func WithFailoverTiming(heartbeatInterval, electionTimeout time.Duration, autoFailover bool) StoreOption {
	return func(c *storeConfig) {
		c.heartbeatInterval = heartbeatInterval
		c.electionTimeout = electionTimeout
		c.autoFailover = autoFailover
	}
}

func buildStoreOptions(cfg *storeConfig) []Option {
	if cfg == nil {
		return nil
	}
	return []Option{WithTiming(cfg.heartbeatInterval, cfg.electionTimeout, cfg.autoFailover)}
}

// buildReplicaSets constructs sets either empty (records == nil and no data
// directory) or restored from durable images. The returned specs are mutated
// to the membership order restored from committed config entries.
func buildReplicaSets(specs []ShardSpec, cfg *storeConfig) (map[string]*ReplicaSet, []string, error) {
	var durable durability
	if cfg != nil && cfg.dataDir != "" {
		disk, err := newDiskDurability(cfg.dataDir)
		if err != nil {
			return nil, nil, err
		}
		durable = disk
	}

	sets := make(map[string]*ReplicaSet, len(specs))
	order := make([]string, 0, len(specs))
	for i := range specs {
		spec := specs[i]
		options := buildStoreOptions(cfg)
		if durable != nil {
			options = append(options, withDurability(durable))
			loader, ok := durable.(interface {
				loadShard(string, []string) (map[string]DurableRecord, []string, error)
			})
			if !ok {
				return nil, nil, newError(KindDurability, "restore", spec.ID,
					"durability backend does not support restore")
			}
			records, restoredOrder, err := loader.loadShard(spec.ID, spec.ReplicaIDs)
			if err != nil {
				return nil, nil, err
			}
			set, err := newReplicaSetForRestore(spec, restoredOrder, records, options...)
			if err != nil {
				return nil, nil, err
			}
			specs[i].ReplicaIDs = append([]string(nil), restoredOrder...)
			sets[spec.ID] = set
		} else {
			set, err := NewReplicaSet(spec, options...)
			if err != nil {
				return nil, nil, err
			}
			sets[spec.ID] = set
		}
		order = append(order, spec.ID)
	}
	return sets, order, nil
}

// NewStoreWithOptions constructs a store with persistent storage and/or
// custom failover timing. A data directory survives a full process stop.
func NewStoreWithOptions(specs []ShardSpec, options ...StoreOption) (*Store, error) {
	cfg := &storeConfig{autoFailover: true}
	for _, opt := range options {
		opt(cfg)
	}
	ordered, err := validateAndOrderSpecs(specs)
	if err != nil {
		return nil, err
	}
	sets, order, err := buildReplicaSets(ordered, cfg)
	if err != nil {
		return nil, err
	}
	return &Store{specs: ordered, sets: sets, order: order}, nil
}

// RestartStore rebuilds the whole store from a data directory previously used
// with WithDataDirectory. Restart semantics:
//
//   - only committed log prefixes are recovered; writes that briefly existed
//     on a minority but were never confirmed are gone and cannot reappear;
//   - request ID dedupe results and per-key versions are rebuilt from the
//     committed log, so versions continue upward and retries stay idempotent;
//   - replicas added by committed expansion entries are rediscovered from
//     disk; an expansion interrupted before its config entry committed stays
//     at the previous configuration;
//   - all replicas start alive with cleared partitions; the background loop
//     re-establishes a leader within the election timeout, after which the
//     system is again readable and writable.
func RestartStore(specs []ShardSpec, dataDir string, options ...StoreOption) (*Store, error) {
	allOptions := append([]StoreOption{WithDataDirectory(dataDir)}, options...)
	return NewStoreWithOptions(specs, allOptions...)
}

// validateAndOrderSpecs contains the configuration validation shared by the
// in-memory and persistent constructors.
func validateAndOrderSpecs(specs []ShardSpec) ([]ShardSpec, error) {
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
	return ordered, nil
}

// Close stops background loops on every shard and performs a final flush of
// committed mirrors when persistence is enabled. Committed state was already
// flushed when each entry committed, so a hard kill also loses nothing
// committed; Close is idempotent and returns the first flush error, if any.
func (s *Store) Close() error {
	var firstErr error
	for _, id := range s.order {
		set := s.sets[id]
		set.mu.Lock()
		if err := set.flushLocked(); err != nil && firstErr == nil {
			firstErr = err
		}
		set.mu.Unlock()
		set.Close()
	}
	return firstErr
}

// SetLogger assigns the same diagnostics logger to every shard.
func (s *Store) SetLogger(logger Logger) {
	for _, id := range s.order {
		s.sets[id].SetLogger(logger)
	}
}

// Specs returns the validated shard specifications in range order.
func (s *Store) Specs() []ShardSpec {
	s.mu.RLock()
	defer s.mu.RUnlock()
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
