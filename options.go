package quorum

import "time"

// Options configures optional long-running-service capabilities without
// changing the default zero-dependency in-memory behavior:
//
//   - DataDir empty keeps the store purely in memory, exactly as before.
//   - Non-empty DataDir persists each replica's committed log prefix, term,
//     vote and the shard manifest, so a full process restart reconstructs the
//     exact committed state.
//   - TickInterval drives the background supervisor that performs automatic
//     leader takeover without client traffic. Zero selects the package
//     default.
type Options struct {
	DataDir      string
	TickInterval time.Duration
	Logger       Logger
}

// DefaultTickInterval is the package default for the background supervisor.
const DefaultTickInterval = 50 * time.Millisecond

// NewStoreWithOptions constructs a store with the given options. The default
// options (zero value) keep the historical in-memory, request-driven behavior;
// the background supervisor is still enabled so leader takeover happens
// without client traffic.
func NewStoreWithOptions(specs []ShardSpec, opts Options) (*Store, error) {
	tick := opts.TickInterval
	if tick <= 0 {
		tick = DefaultTickInterval
	}
	store, err := NewStore(specs)
	if err != nil {
		return nil, err
	}
	if opts.Logger != nil {
		store.SetLogger(opts.Logger)
	}
	store.persister = newPersistence(opts.DataDir)
	store.tick = tick
	for _, id := range store.order {
		set := store.sets[id]
		set.mu.Lock()
		set.persister = store.persister
		set.tickInterval = tick
		if err := set.restoreLocked(); err != nil {
			set.mu.Unlock()
			store.Close()
			return nil, err
		}
		// Persist initial membership when no durable manifest exists yet,
		// so the on-disk layout is complete from the first run.
		if store.persister.enabled() {
			if _, has, err := store.persister.loadManifest(set.spec.ID); err != nil {
				set.mu.Unlock()
				store.Close()
				return nil, err
			} else if !has {
				if err := store.persister.saveManifest(shardManifest{
					ShardID: set.spec.ID,
					Voters:  append([]string(nil), set.voters...),
				}); err != nil {
					set.mu.Unlock()
					store.Close()
					return nil, err
				}
			}
		}
		set.running = true
		set.stopCh = make(chan struct{})
		set.startSupervisorLocked()
		set.mu.Unlock()
	}
	store.running = true
	store.stopCh = make(chan struct{})
	return store, nil
}

// Close stops the background supervisors of every shard. In-memory stores
// started via NewStore do not start background work and need no Close.
func (s *Store) Close() error {
	if !s.running {
		return nil
	}
	for _, id := range s.order {
		s.sets[id].closeSupervisor()
	}
	s.running = false
	return nil
}
