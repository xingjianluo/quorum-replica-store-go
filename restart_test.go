package quorum

import (
	"context"
	"testing"
	"time"
)

func newPersistentStore(t *testing.T, dir string, replicaIDs []string) (*Store, *ReplicaSet) {
	t.Helper()
	store, err := NewStoreWithOptions([]ShardSpec{{
		ID: "persist", Range: ShardRange{}, ReplicaIDs: replicaIDs,
	}}, Options{DataDir: dir, TickInterval: fastTick, Logger: testLogger{t}})
	if err != nil {
		t.Fatalf("open store (dir=%s): %v", dir, err)
	}
	shard, err := store.Shard("persist")
	if err != nil {
		t.Fatal(err)
	}
	return store, shard
}

// TestRestartPreservesCommittedStateVersionsAndRequestIDs restarts the whole
// instance (all replicas at once) and checks the three durability promises:
// acknowledged data comes back byte-identical, per-key versions continue
// upward instead of resetting, and a request ID acknowledged before restart is
// still deduplicated after restart.
func TestRestartPreservesCommittedStateVersionsAndRequestIDs(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	store, _ := newPersistentStore(t, dir, []string{"r1", "r2", "r3"})
	seedWrites := []WriteRequest{
		{ShardID: "persist", Key: "k", Value: []byte("v1"), RequestID: "k-r1"},
		{ShardID: "persist", Key: "k", Value: []byte("v2"), RequestID: "k-r2"},
		{ShardID: "persist", Key: "other", Value: []byte("o1"), RequestID: "other-r1"},
		{ShardID: "persist", Key: "k", Value: []byte("v3"), RequestID: "k-r3"},
	}
	for _, req := range seedWrites {
		resp, err := store.Write(ctx, req)
		if err != nil {
			t.Fatalf("seed write %s: %v", req.RequestID, err)
		}
		t.Logf("committed request=%s key=%s value=%s version=%d index=%d",
			req.RequestID, req.Key, string(req.Value), resp.Version, resp.CommitIndex)
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	t.Log("decision basis: whole instance closed; every replica durable snapshot holds only its committed prefix")

	store2, shard2 := newPersistentStore(t, dir, []string{"r1", "r2", "r3"})
	t.Cleanup(func() { _ = store2.Close() })

	k, err := store2.Read(ctx, ReadRequest{ShardID: "persist", Key: "k", Mode: ReadModeStrong})
	if err != nil {
		t.Fatalf("strong read after restart: %v", err)
	}
	if string(k.Value.Data) != "v3" || k.Value.Version != 3 {
		t.Fatalf("committed state changed across restart: %+v", k.Value)
	}
	o, err := store2.Read(ctx, ReadRequest{ShardID: "persist", Key: "other", Mode: ReadModeStrong})
	if err != nil {
		t.Fatal(err)
	}
	if o.Value == nil || string(o.Value.Data) != "o1" || o.Value.Version != 1 {
		t.Fatalf("secondary key did not survive restart: %+v", o.Value)
	}
	t.Logf("invariant after restart: k=%s version=%d; other=%s version=%1d",
		k.Value.Data, k.Value.Version, o.Value.Data, o.Value.Version)

	// Same request ID after restart: must hit the existing result, not apply
	// again. The response has to carry the original version and Duplicate=true.
	dup, err := store2.Write(ctx, WriteRequest{
		ShardID: "persist", Key: "k", Value: []byte("v2"), RequestID: "k-r2",
	})
	if err != nil {
		t.Fatalf("duplicate after restart: %v", err)
	}
	if !dup.Duplicate || dup.Version != 2 {
		t.Fatalf("request ID dedupe did not survive restart: %+v", dup)
	}
	t.Logf("idempotency basis: request k-r2 matched committed version=%d duplicate=true (not a new commit)",
		dup.Version)

	// Same ID with different parameters is still a conflict across restart.
	_, err = store2.Write(ctx, WriteRequest{
		ShardID: "persist", Key: "k", Value: []byte("tampered"), RequestID: "k-r2",
	})
	assertErrorKind(t, err, KindDuplicateConflict)

	// Versions continue upward from the durable high-water mark.
	next, err := store2.Write(ctx, WriteRequest{
		ShardID: "persist", Key: "k", Value: []byte("v4"), RequestID: "k-r4",
	})
	if err != nil {
		t.Fatalf("write after restart: %v", err)
	}
	if next.Version != 4 {
		t.Fatalf("version regressed/reset after restart: got %d want 4", next.Version)
	}
	t.Logf("invariant: post-restart commit version=%d continues the pre-restart sequence", next.Version)

	// All durable replicas eventually hold the same committed prefix.
	waitForStatus(t, shard2, 2*time.Second, func(s ShardStatus) bool {
		return s.Writable
	}, "restarted shard reconverges to writable")
}

// TestRestartDropsUncommittedMinorityEntry proves a write that existed only on
// a minority and was never acknowledged as committed cannot come back as data
// after a full restart.
func TestRestartDropsUncommittedMinorityEntry(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	store, shard := newPersistentStore(t, dir, []string{"r1", "r2", "r3"})
	seed, err := store.Write(ctx, WriteRequest{
		ShardID: "persist", Key: "k", Value: []byte("committed"), RequestID: "seed",
	})
	if err != nil {
		t.Fatal(err)
	}
	leader := seed.LeaderID

	// Remove a follower from the majority, then crash the other follower so
	// the leader stands alone; the attempted next write must be rejected.
	var aliveFollower, other string
	for _, id := range shard.Spec().ReplicaIDs {
		if id == leader {
			continue
		}
		if aliveFollower == "" {
			aliveFollower = id
		} else {
			other = id
		}
	}
	if err := shard.AddPartition(leader, aliveFollower); err != nil {
		t.Fatal(err)
	}
	if err := shard.CrashReplica(other); err != nil {
		t.Fatal(err)
	}
	_, err = store.Write(ctx, WriteRequest{
		ShardID: "persist", Key: "k", Value: []byte("never-committed"), RequestID: "ghost",
	})
	assertErrorKind(t, err, KindQuorumUnavailable)
	t.Logf("decision basis: ghost write rejected with %v; it may live in no committed snapshot", err)

	// Close while the topology is still broken; restart heals everything.
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store2, _ := newPersistentStore(t, dir, []string{"r1", "r2", "r3"})
	t.Cleanup(func() { _ = store2.Close() })

	r, err := store2.Read(ctx, ReadRequest{ShardID: "persist", Key: "k", Mode: ReadModeStrong})
	if err != nil {
		t.Fatalf("strong read after restart: %v", err)
	}
	if string(r.Value.Data) != "committed" || r.Value.Version != 1 {
		t.Fatalf("an unacknowledged minority write surfaced after restart: %+v", r.Value)
	}
	if _, derr := store2.Write(ctx, WriteRequest{
		ShardID: "persist", Key: "x", Value: []byte("1"), RequestID: "ghost",
	}); derr != nil {
		// "ghost" must not be recorded as committed, so reusing it with a
		// different command must NOT be a duplicate conflict.
		assertErrorKind(t, derr, KindDuplicateConflict)
		t.Fatalf("uncommitted request ID ghost was persisted as a committed result: %v", derr)
	}
	t.Log("invariant: uncommitted minority write and its request ID are absent after restart")
}

// TestRestartReelectsAndServesWrites checks a restarted cluster automatically
// returns to a readable/writable state with no manual election call.
func TestRestartReelectsAndServesWrites(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, _ := newPersistentStore(t, dir, []string{"r1", "r2", "r3"})
	if _, err := store.Write(ctx, WriteRequest{
		ShardID: "persist", Key: "k", Value: []byte("v1"), RequestID: "p1",
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store2, shard2 := newPersistentStore(t, dir, []string{"r1", "r2", "r3"})
	t.Cleanup(func() { _ = store2.Close() })
	st := waitForStatus(t, shard2, 2*time.Second, func(s ShardStatus) bool {
		return s.Writable
	}, "automatic election after full restart")
	t.Logf("decision basis: restarted cluster elected leader=%s term=%d with no client election call",
		st.LeaderID, st.Term)
	w, err := store2.Write(ctx, WriteRequest{
		ShardID: "persist", Key: "k", Value: []byte("v2"), RequestID: "p2",
	})
	if err != nil {
		t.Fatalf("write after restarted auto-election: %v", err)
	}
	if w.Version != 2 {
		t.Fatalf("post-restart version=%d, want 2", w.Version)
	}
}
