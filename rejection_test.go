package quorum

import (
	"context"
	"testing"
)

func TestDistinguishableRejectionReasons(t *testing.T) {
	ctx := context.Background()
	store, err := NewStore([]ShardSpec{
		{ID: "low", Range: ShardRange{Start: "", End: "m"}, ReplicaIDs: []string{"a1", "a2", "a3"}},
		{ID: "high", Range: ShardRange{Start: "m", End: ""}, ReplicaIDs: []string{"b1", "b2", "b3"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	store.SetLogger(testLogger{t})
	t.Cleanup(func() { _ = store.Close() })
	low, err := store.Shard("low")
	if err != nil {
		t.Fatal(err)
	}

	_, err = store.Write(ctx, WriteRequest{ShardID: "low", Key: "z", Value: []byte("x"), RequestID: "wrong-shard"})
	assertErrorKind(t, err, KindKeyOutOfRange)

	_, err = store.Write(ctx, WriteRequest{ShardID: "high", Key: "a", Value: []byte("x"), RequestID: "wrong-shard-2"})
	assertErrorKind(t, err, KindKeyOutOfRange)

	_, err = store.Read(ctx, ReadRequest{ShardID: "low", Key: "a", Mode: ReadMode(99)})
	assertErrorKind(t, err, KindInvalidReadMode)

	strong, err := store.Read(ctx, ReadRequest{ShardID: "low", Key: "a", Mode: ReadModeStrong})
	if err != nil {
		t.Fatalf("initial strong read: %v", err)
	}
	if strong.Value != nil || strong.Stale {
		t.Fatalf("unexpected initial read: %+v", strong)
	}

	v0 := uint64(0)
	if _, err := store.Write(ctx, WriteRequest{
		ShardID: "low", Key: "a", Value: []byte("v1"),
		CompareVersion: &v0, RequestID: "create",
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	v99 := uint64(99)
	_, err = store.Write(ctx, WriteRequest{
		ShardID: "low", Key: "a", Value: []byte("bad"),
		CompareVersion: &v99, RequestID: "bad-cas",
	})
	assertErrorKind(t, err, KindVersionConflict)

	_, err = store.Write(ctx, WriteRequest{
		ShardID: "low", Key: "a", Value: []byte("different"), RequestID: "create",
	})
	assertErrorKind(t, err, KindDuplicateConflict)

	_, err = store.Write(ctx, WriteRequest{ShardID: "low", Key: "b", Value: []byte("no-id")})
	assertErrorKind(t, err, KindMissingRequestID)

	if err := low.CrashReplica("a2"); err != nil {
		t.Fatal(err)
	}
	if err := low.CrashReplica("a3"); err != nil {
		t.Fatal(err)
	}
	_, err = store.Write(ctx, WriteRequest{ShardID: "low", Key: "a", Value: []byte("v2"), RequestID: "minority-write"})
	assertErrorKind(t, err, KindQuorumUnavailable)

	_, err = store.Read(ctx, ReadRequest{ShardID: "low", Key: "a", Mode: ReadModeStrong})
	assertErrorKind(t, err, KindQuorumUnavailable)

	eventual, err := store.Read(ctx, ReadRequest{
		ShardID: "low", Key: "a", Mode: ReadModeEventual, ReplicaID: "a1",
	})
	if err != nil {
		t.Fatalf("eventual read from sole alive minority replica: %v", err)
	}
	if string(eventual.Value.Data) != "v1" || !eventual.Stale {
		t.Fatalf("eventual minority read should expose last committed value as potentially stale: %+v", eventual)
	}
}
