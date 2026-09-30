package quorum

import (
	"context"
	"testing"
	"time"
)

func TestClientRetriesSameRequestIDUntilMajorityRestored(t *testing.T) {
	ctx := context.Background()
	store, shard := newFiveReplicaStore(t)
	client := NewClient(store, 20, 5*time.Millisecond)
	client.SetLogger(testLogger{t})

	first, err := client.Write(ctx, WriteRequest{ShardID: "five", Key: "retry", Value: []byte("v1"), RequestID: "seed"})
	if err != nil {
		t.Fatal(err)
	}

	crashed := []string{first.LeaderID}
	for _, id := range shard.Spec().ReplicaIDs {
		if id != first.LeaderID && len(crashed) < 3 {
			crashed = append(crashed, id)
		}
	}
	for _, id := range crashed {
		if err := shard.CrashReplica(id); err != nil {
			t.Fatal(err)
		}
	}

	type result struct {
		resp *WriteResponse
		err  error
	}
	resultCh := make(chan result, 1)
	go func() {
		resp, err := client.Write(ctx, WriteRequest{
			ShardID:   "five",
			Key:       "retry",
			Value:     []byte("v2"),
			RequestID: "retry-once",
		})
		resultCh <- result{resp: resp, err: err}
	}()

	time.Sleep(30 * time.Millisecond)
	for _, id := range crashed {
		if err := shard.RecoverReplica(id); err != nil {
			t.Fatal(err)
		}
	}

	select {
	case got := <-resultCh:
		if got.err != nil {
			t.Fatalf("retried write after majority recovery: %v", got.err)
		}
		if got.resp.Version != 2 || got.resp.Duplicate {
			t.Fatalf("retried request was not committed exactly once: %+v", got.resp)
		}
		again, err := client.Write(ctx, WriteRequest{
			ShardID: "five", Key: "retry", Value: []byte("v2"), RequestID: "retry-once",
		})
		if err != nil {
			t.Fatalf("duplicate after successful retry: %v", err)
		}
		if !again.Duplicate || again.Version != 2 || again.CommitIndex != got.resp.CommitIndex {
			t.Fatalf("retry request ID was reapplied: first=%+v duplicate=%+v", got.resp, again)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("client did not finish after majority recovered")
	}
}
