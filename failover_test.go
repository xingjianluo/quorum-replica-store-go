package quorum

import (
	"context"
	"errors"
	"testing"
)

func assertErrorKind(t *testing.T, err error, want ErrorKind) {
	t.Helper()
	var qerr *Error
	if !errors.As(err, &qerr) {
		t.Fatalf("error %v is not a quorum.Error", err)
	}
	if qerr.Kind != want {
		t.Fatalf("error kind = %q, want %q; full error: %v", qerr.Kind, want, err)
	}
}

func readData(t *testing.T, store *Store, shardID, key string, mode ReadMode, replicaID string) (string, *ReadResponse) {
	t.Helper()
	resp, err := store.Read(context.Background(), ReadRequest{
		ShardID:   shardID,
		Key:       key,
		Mode:      mode,
		ReplicaID: replicaID,
	})
	if err != nil {
		t.Fatalf("Read mode=%d replica=%s: %v", mode, replicaID, err)
	}
	if resp.Value == nil {
		t.Fatalf("Read mode=%d replica=%s returned nil committed value", mode, replicaID)
	}
	return string(resp.Value.Data), resp
}

func TestLeaderCrashElectionAndOldLeaderCatchUp(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	client := NewClient(store, 3, 0)
	client.SetLogger(testLogger{t})
	shard, err := store.Shard("users")
	if err != nil {
		t.Fatal(err)
	}

	first, err := client.Write(ctx, WriteRequest{
		ShardID: "users", Key: "k", Value: []byte("v1"), RequestID: "r1",
	})
	if err != nil {
		t.Fatalf("initial write: %v", err)
	}
	oldLeader := first.LeaderID
	if oldLeader == "" {
		t.Fatal("initial response has no leader")
	}

	if err := shard.CrashReplica(oldLeader); err != nil {
		t.Fatalf("crash leader: %v", err)
	}
	second, err := client.Write(ctx, WriteRequest{
		ShardID: "users", Key: "k", Value: []byte("v2"), RequestID: "r2",
	})
	if err != nil {
		t.Fatalf("write after leader crash: %v", err)
	}
	if second.LeaderID == oldLeader {
		t.Fatalf("new write was served by crashed leader %s", oldLeader)
	}
	if second.Version != 2 || second.CommitIndex <= first.CommitIndex {
		t.Fatalf("new leader revision did not advance: first=%+v second=%+v", first, second)
	}

	data, strong := readData(t, store, "users", "k", ReadModeStrong, "")
	if data != "v2" {
		t.Fatalf("strong read after failover = %q, want v2", data)
	}
	if strong.ReplicaID == oldLeader {
		t.Fatalf("strong read was served by crashed old leader: %+v", strong)
	}

	if err := shard.RecoverReplica(oldLeader); err != nil {
		t.Fatalf("recover old leader: %v", err)
	}
	data, stale := readData(t, store, "users", "k", ReadModeEventual, oldLeader)
	if data != "v1" {
		t.Fatalf("old leader changed committed data before catch-up: got %q, want retained v1", data)
	}
	if !stale.Stale {
		t.Fatalf("recovered old leader was not marked stale before catch-up: %+v", stale)
	}

	if err := shard.CatchUp(); err != nil {
		t.Fatalf("catch-up old leader: %v", err)
	}
	data, caughtUp := readData(t, store, "users", "k", ReadModeEventual, oldLeader)
	if data != "v2" {
		t.Fatalf("old leader after catch-up = %q, want v2; invariant: committed data must converge", data)
	}
	if caughtUp.Stale || caughtUp.CommitIndex < strong.CommitIndex {
		t.Fatalf("old leader did not converge: recovered=%+v current strong=%+v", caughtUp, strong)
	}
	if shard.LeaderID() != second.LeaderID {
		t.Fatalf("catch-up replaced leader: got %s want %s; recovered old leader must not overwrite new leader",
			shard.LeaderID(), second.LeaderID)
	}
}

func TestLongOfflineReplicaCatchesUpMultipleCommittedRevisions(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	client := NewClient(store, 3, 0)
	client.SetLogger(testLogger{t})
	shard, err := store.Shard("users")
	if err != nil {
		t.Fatal(err)
	}

	first, err := client.Write(ctx, WriteRequest{
		ShardID: "users", Key: "offline", Value: []byte("v0"), RequestID: "off-0",
	})
	if err != nil {
		t.Fatal(err)
	}
	offline := "r3"
	if first.LeaderID == offline {
		offline = "r2"
	}
	if err := shard.CrashReplica(offline); err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= 5; i++ {
		_, err := client.Write(ctx, WriteRequest{
			ShardID:   "users",
			Key:       "offline",
			Value:     []byte(string(rune('0' + i))),
			RequestID: "off-" + string(rune('0'+i)),
		})
		if err != nil {
			t.Fatalf("write revision %d while replica offline: %v", i, err)
		}
	}
	if _, err := client.Write(ctx, WriteRequest{
		ShardID: "users", Key: "other", Value: []byte("side-write"), RequestID: "off-side",
	}); err != nil {
		t.Fatal(err)
	}

	if err := shard.RecoverReplica(offline); err != nil {
		t.Fatal(err)
	}
	if err := shard.CatchUp(); err != nil {
		t.Fatalf("catch-up long offline replica: %v", err)
	}

	main, err := store.Read(ctx, ReadRequest{
		ShardID: "users", Key: "offline", Mode: ReadModeEventual, ReplicaID: offline,
	})
	if err != nil {
		t.Fatal(err)
	}
	other, err := store.Read(ctx, ReadRequest{
		ShardID: "users", Key: "other", Mode: ReadModeEventual, ReplicaID: offline,
	})
	if err != nil {
		t.Fatal(err)
	}
	if main.Value == nil || string(main.Value.Data) != "5" || main.Value.Version != 6 || main.Stale {
		t.Fatalf("offline replica did not converge main key: %+v", main.Value)
	}
	if other.Value == nil || string(other.Value.Data) != "side-write" || other.Value.Version != 1 {
		t.Fatalf("offline replica missed side committed key: %+v", other.Value)
	}
	if main.CommitIndex != shard.Status()[shard.LeaderID()].CommitIndex {
		t.Fatalf("offline commit=%d differs from leader commit=%d",
			main.CommitIndex, shard.Status()[shard.LeaderID()].CommitIndex)
	}
}
