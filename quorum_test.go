package quorum

import (
	"context"
	"testing"
)

func newFiveReplicaStore(t *testing.T) (*Store, *ReplicaSet) {
	t.Helper()
	store, err := NewStore([]ShardSpec{{
		ID: "five", Range: ShardRange{}, ReplicaIDs: []string{"n1", "n2", "n3", "n4", "n5"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	store.SetLogger(testLogger{t})
	shard, err := store.Shard("five")
	if err != nil {
		t.Fatal(err)
	}
	return store, shard
}

func TestFiveReplicaQuorumBoundary(t *testing.T) {
	ctx := context.Background()
	store, shard := newFiveReplicaStore(t)
	client := NewClient(store, 3, 0)
	client.SetLogger(testLogger{t})

	first, err := client.Write(ctx, WriteRequest{ShardID: "five", Key: "q", Value: []byte("v1"), RequestID: "q1"})
	if err != nil {
		t.Fatalf("initial write: %v", err)
	}

	crashed := []string{}
	for _, id := range shard.Spec().ReplicaIDs {
		if id != first.LeaderID && len(crashed) < 2 {
			if err := shard.CrashReplica(id); err != nil {
				t.Fatal(err)
			}
			crashed = append(crashed, id)
		}
	}
	second, err := client.Write(ctx, WriteRequest{ShardID: "five", Key: "q", Value: []byte("v2"), RequestID: "q2"})
	if err != nil {
		t.Fatalf("write with 3/5 alive: %v", err)
	}
	if second.Version != 2 || second.LeaderID != first.LeaderID {
		t.Fatalf("3/5 alive should retain the same leader and advance revision: first=%+v second=%+v", first, second)
	}

	alive := []string{}
	for id, status := range shard.Status() {
		if status.Alive {
			alive = append(alive, id)
		}
	}
	for _, id := range alive {
		if id != first.LeaderID && len(crashed) < 3 {
			if err := shard.CrashReplica(id); err != nil {
				t.Fatal(err)
			}
			crashed = append(crashed, id)
		}
	}
	_, err = store.Write(ctx, WriteRequest{ShardID: "five", Key: "q", Value: []byte("v3"), RequestID: "q3"})
	assertErrorKind(t, err, KindQuorumUnavailable)

	_, err = store.Read(ctx, ReadRequest{ShardID: "five", Key: "q", Mode: ReadModeStrong})
	assertErrorKind(t, err, KindQuorumUnavailable)

	for _, id := range crashed[:2] {
		if err := shard.RecoverReplica(id); err != nil {
			t.Fatal(err)
		}
	}
	if err := shard.CatchUp(); err != nil {
		t.Fatalf("catch-up after restoring majority: %v", err)
	}
	third, err := client.Write(ctx, WriteRequest{ShardID: "five", Key: "q", Value: []byte("v3"), RequestID: "q3"})
	if err != nil {
		t.Fatalf("write after restoring majority: %v", err)
	}
	if third.Version != 3 {
		t.Fatalf("revision after recovery = %d, want 3", third.Version)
	}
	if _, err := client.Write(ctx, WriteRequest{ShardID: "five", Key: "q", Value: []byte("v3"), RequestID: "q3"}); err != nil {
		t.Fatalf("retry of previously rejected request after recovery: %v", err)
	}
	last, err := store.Read(ctx, ReadRequest{ShardID: "five", Key: "q", Mode: ReadModeStrong})
	if err != nil {
		t.Fatal(err)
	}
	if string(last.Value.Data) != "v3" || last.Value.Version != 3 {
		t.Fatalf("final committed value = %+v, want v3 version=3", last.Value)
	}
}
