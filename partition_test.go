package quorum

import (
	"context"
	"testing"
)

func TestNetworkPartitionElectsMajorityLeaderAndConvergesAfterHeal(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	client := NewClient(store, 3, 0)
	client.SetLogger(testLogger{t})
	shard, err := store.Shard("users")
	if err != nil {
		t.Fatal(err)
	}

	first, err := client.Write(ctx, WriteRequest{
		ShardID: "users", Key: "partitioned", Value: []byte("before"), RequestID: "p1",
	})
	if err != nil {
		t.Fatalf("write before partition: %v", err)
	}
	oldLeader := first.LeaderID
	others := make([]string, 0, 2)
	for _, id := range []string{"r1", "r2", "r3"} {
		if id != oldLeader {
			others = append(others, id)
		}
	}
	if err := shard.AddPartition(oldLeader, others[0]); err != nil {
		t.Fatal(err)
	}
	if err := shard.AddPartition(oldLeader, others[1]); err != nil {
		t.Fatal(err)
	}

	second, err := client.Write(ctx, WriteRequest{
		ShardID: "users", Key: "partitioned", Value: []byte("during"), RequestID: "p2",
	})
	if err != nil {
		t.Fatalf("write through majority side: %v", err)
	}
	if second.LeaderID == oldLeader {
		t.Fatalf("partitioned replica %s remained serving leader", oldLeader)
	}
	if second.CommitTerm <= first.CommitTerm {
		t.Fatalf("majority-side term did not advance: first=%+v second=%+v", first, second)
	}

	strong, err := client.Read(ctx, ReadRequest{ShardID: "users", Key: "partitioned", Mode: ReadModeStrong})
	if err != nil {
		t.Fatalf("strong read during partition: %v", err)
	}
	if strong.ReplicaID == oldLeader || string(strong.Value.Data) != "during" {
		t.Fatalf("strong read did not use majority side: %+v", strong)
	}

	oldView, err := store.Read(ctx, ReadRequest{
		ShardID: "users", Key: "partitioned", Mode: ReadModeEventual, ReplicaID: oldLeader,
	})
	if err != nil {
		t.Fatalf("eventual read on isolated old leader: %v", err)
	}
	if string(oldView.Value.Data) != "before" || !oldView.Stale {
		t.Fatalf("isolated old leader view should remain last committed and be marked stale: %+v", oldView)
	}

	shard.HealPartitions()
	if err := shard.CatchUp(); err != nil {
		t.Fatalf("catch-up after heal: %v", err)
	}
	caughtUp, err := store.Read(ctx, ReadRequest{
		ShardID: "users", Key: "partitioned", Mode: ReadModeEventual, ReplicaID: oldLeader,
	})
	if err != nil {
		t.Fatalf("read old leader after heal: %v", err)
	}
	if string(caughtUp.Value.Data) != "during" || caughtUp.Stale ||
		caughtUp.CommitIndex < strong.CommitIndex || caughtUp.CommitTerm < strong.CommitTerm {
		t.Fatalf("old leader did not converge after partition healed: old=%+v strong=%+v", caughtUp, strong)
	}
	if shard.LeaderID() != second.LeaderID || shard.Term() != second.CommitTerm {
		t.Fatalf("heal/catch-up changed committed leadership: leader=%s term=%d want leader=%s term=%d",
			shard.LeaderID(), shard.Term(), second.LeaderID, second.CommitTerm)
	}
}
