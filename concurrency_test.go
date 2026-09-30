package quorum

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

func TestConcurrentWritesAreCommittedExactlyOnceAndConverge(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	client := NewClient(store, 3, 0)
	client.SetLogger(testLogger{t})
	shard, err := store.Shard("users")
	if err != nil {
		t.Fatal(err)
	}

	const n = 20
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := client.Write(ctx, WriteRequest{
				ShardID:   "users",
				Key:       fmt.Sprintf("key-%02d", i),
				Value:     []byte(fmt.Sprintf("value-%02d", i)),
				RequestID: fmt.Sprintf("concurrent-%02d", i),
			})
			if err != nil {
				errCh <- err
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent write: %v", err)
	}

	if err := shard.CatchUp(); err != nil {
		t.Fatalf("catch-up: %v", err)
	}
	statuses := shard.Status()
	var commit uint64
	for id, status := range statuses {
		if !status.Alive {
			t.Fatalf("replica %s unexpectedly crashed", id)
		}
		if commit == 0 {
			commit = status.CommitIndex
		}
		if status.CommitIndex != commit {
			t.Fatalf("invariant violated: replica %s commit=%d, want %d", id, status.CommitIndex, commit)
		}
	}
	// One election no-op plus 20 user writes.
	if want := uint64(n + 1); commit != want {
		t.Fatalf("commit index = %d, want %d; duplicate or missing write suspected", commit, want)
	}

	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%02d", i)
		want := fmt.Sprintf("value-%02d", i)
		for _, replicaID := range []string{"r1", "r2", "r3"} {
			resp, err := store.Read(ctx, ReadRequest{
				ShardID: "users", Key: key, Mode: ReadModeEventual, ReplicaID: replicaID,
			})
			if err != nil {
				t.Fatalf("read %s from %s: %v", key, replicaID, err)
			}
			if resp.Value == nil || string(resp.Value.Data) != want || resp.Value.Version != 1 {
				t.Fatalf("replica %s key %s = %+v, want version=1 data=%q",
					replicaID, key, resp.Value, want)
			}
		}
	}
}

func TestConcurrentStrongReadsNeverObserveAmbiguousState(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	writer := NewClient(store, 1, 0)
	reader := NewClient(store, 3, 0)
	writer.SetLogger(testLogger{t})
	reader.SetLogger(testLogger{t})

	if _, err := writer.Write(ctx, WriteRequest{
		ShardID: "users", Key: "hot", Value: []byte("init"), RequestID: "init",
	}); err != nil {
		t.Fatal(err)
	}

	const writers = 8
	const readers = 8
	var wg sync.WaitGroup
	stop := make(chan struct{})
	start := make(chan struct{})
	for w := 0; w < writers; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				_, _ = writer.Write(ctx, WriteRequest{
					ShardID:   "users",
					Key:       fmt.Sprintf("hot-writer-%d", w),
					Value:     []byte(fmt.Sprintf("v-%d", i)),
					RequestID: fmt.Sprintf("w-%d-%d", w, i),
				})
			}
		}()
	}
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for {
				select {
				case <-stop:
					return
				default:
				}
				resp, err := reader.Read(ctx, ReadRequest{ShardID: "users", Key: "hot", Mode: ReadModeStrong})
				if err != nil {
					t.Errorf("strong read: %v", err)
					return
				}
				if resp.Stale || resp.Value == nil || string(resp.Value.Data) != "init" {
					t.Errorf("strong read returned invalid state: %+v", resp)
					return
				}
			}
		}()
	}
	close(start)
	// The store is serialized deterministically; this mainly exercises lock use
	// and concurrent request handling under -race.
	for i := 0; i < 200; i++ {
		_, _ = writer.Write(ctx, WriteRequest{
			ShardID: "users", Key: "hot", Value: []byte("init"), RequestID: fmt.Sprintf("read-load-%d", i),
		})
	}
	close(stop)
	wg.Wait()
}
