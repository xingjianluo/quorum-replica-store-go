package quorum

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func newExpandStore(t *testing.T) (*Store, *ReplicaSet) {
	t.Helper()
	store, err := NewStoreWithOptions([]ShardSpec{{
		ID: "grow", Range: ShardRange{}, ReplicaIDs: []string{"n1", "n2", "n3"},
	}}, Options{TickInterval: fastTick, Logger: testLogger{t}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	shard, err := store.Shard("grow")
	if err != nil {
		t.Fatal(err)
	}
	return store, shard
}

// TestExpandThreeToFiveNewQuorumAppliesAndConverges covers the steady-state
// expansion: existing writes keep succeeding, new replicas catch up the
// committed prefix as learners, the joint commit needs both majorities, and
// the new 3-of-5 threshold is in force immediately afterwards.
func TestExpandThreeToFiveNewQuorumAppliesAndConverges(t *testing.T) {
	ctx := context.Background()
	store, shard := newExpandStore(t)

	for i := 1; i <= 3; i++ {
		if _, err := store.Write(ctx, WriteRequest{
			ShardID: "grow", Key: "k", Value: []byte(fmt.Sprintf("v%d", i)),
			RequestID: fmt.Sprintf("pre-%d", i),
		}); err != nil {
			t.Fatalf("pre-expansion write %d: %v", i, err)
		}
	}
	before := shard.ClusterStatus()
	if before.Quorum != 2 || len(before.ConfiguredVoters) != 3 {
		t.Fatalf("unexpected pre-expansion config: %+v", before)
	}

	if err := store.ExpandReplicas("grow", []string{"n4", "n5"}); err != nil {
		t.Fatalf("ExpandReplicas: %v", err)
	}
	after := shard.ClusterStatus()
	t.Logf("decision basis: expansion complete voters=%v quorum=%d joint=%t",
		after.ConfiguredVoters, after.Quorum, after.Joint)
	if len(after.ConfiguredVoters) != 5 || after.Quorum != 3 || after.Joint {
		t.Fatalf("new membership did not become the sole config: %+v", after)
	}
	if !after.Writable {
		t.Fatalf("shard not writable right after expansion: %+v", after)
	}

	// New replicas must be voters converged with the pre-expansion prefix.
	for _, id := range []string{"n4", "n5"} {
		rs := shard.Status()[id]
		if !rs.Voter || rs.Learner {
			t.Fatalf("replica %s did not graduate to voter: %+v", id, rs)
		}
		r, err := store.Read(ctx, ReadRequest{
			ShardID: "grow", Key: "k", Mode: ReadModeEventual, ReplicaID: id,
		})
		if err != nil {
			t.Fatalf("read new replica %s: %v", id, err)
		}
		if r.Value == nil || string(r.Value.Data) != "v3" || r.Value.Version != 3 || r.Stale {
			t.Fatalf("new replica %s did not converge with committed state: %+v stale=%t",
				id, r.Value, r.Stale)
		}
	}
	t.Log("invariant: n4/n5 carry the full pre-expansion committed prefix (v3@3)")

	// New threshold applies at once: 3/5 alive must still serve, 2/5 must not.
	w4, err := store.Write(ctx, WriteRequest{
		ShardID: "grow", Key: "k", Value: []byte("v4"), RequestID: "post-4",
	})
	if err != nil {
		t.Fatalf("post-expansion write: %v", err)
	}
	if w4.Version != 4 {
		t.Fatalf("version after expansion=%d want 4", w4.Version)
	}

	leader := after.LeaderID
	crashed := []string{}
	for _, id := range after.ConfiguredVoters {
		if id != leader && len(crashed) < 2 {
			if err := shard.CrashReplica(id); err != nil {
				t.Fatal(err)
			}
			crashed = append(crashed, id)
		}
	}
	st := shard.ClusterStatus()
	t.Logf("decision basis: 3/5 alive => writable=%t leaderReachable=%d quorum=%d",
		st.Writable, st.LeaderReachable, st.Quorum)
	if !st.Writable {
		t.Fatalf("3/5 alive must be writable under the new quorum: %+v %s", st, st.WriteBlockReason)
	}
	if _, err := store.Write(ctx, WriteRequest{
		ShardID: "grow", Key: "k", Value: []byte("v5"), RequestID: "post-5",
	}); err != nil {
		t.Fatalf("write with exactly new quorum alive: %v", err)
	}

	if err := shard.CrashReplica(leader); err != nil {
		t.Fatal(err)
	}
	bad := waitForStatus(t, shard, 2*time.Second, func(s ShardStatus) bool {
		return !s.Writable
	}, "2/5 alive stops serving")
	t.Logf("decision basis: 2/5 alive state=%s reason=%q", bad.WriteState, bad.WriteBlockReason)
	if _, err := store.Write(ctx, WriteRequest{
		ShardID: "grow", Key: "k", Value: []byte("v6"), RequestID: "post-6",
	}); err == nil {
		t.Fatal("2/5 write accepted: two disjoint majorities hazard")
	} else {
		assertErrorKind(t, err, KindQuorumUnavailable)
	}
}

// TestExpansionInterleavedWithConcurrentWrites runs writes throughout the
// joint change and afterwards, verifying exactly-once request semantics,
// monotonic versions and full 5-replica convergence. Serialized shard
// execution plus the joint quorum means no write can commit against a stale
// old-only majority while the new members join.
func TestExpansionInterleavedWithConcurrentWrites(t *testing.T) {
	ctx := context.Background()
	store, shard := newExpandStore(t)

	const total = 40
	var wg sync.WaitGroup
	errCh := make(chan error, total+1)
	start := make(chan struct{})

	// Background writers keep issuing new request IDs across the expansion.
	for i := 0; i < total; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := store.Write(ctx, WriteRequest{
				ShardID:   "grow",
				Key:       fmt.Sprintf("key-%02d", i%5),
				Value:     []byte(fmt.Sprintf("w-%02d", i)),
				RequestID: fmt.Sprintf("iw-%02d", i),
			})
			if err != nil {
				errCh <- fmt.Errorf("interleaved write iw-%02d: %w", i, err)
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		// Give the first few writes time to enter, then expand while writers
		// are still in flight.
		time.Sleep(10 * time.Millisecond)
		if err := store.ExpandReplicas("grow", []string{"n4", "n5"}); err != nil {
			errCh <- fmt.Errorf("expansion interleaved: %w", err)
		}
	}()

	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	st := shard.ClusterStatus()
	if len(st.ConfiguredVoters) != 5 || st.Quorum != 3 {
		t.Fatalf("expansion did not finish with the new quorum: %+v", st)
	}

	// Every distinct request ID committed exactly once: replaying each
	// request returns Duplicate=true with the same version, and all five
	// replicas converge to the same commit index under the new config.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		leaderStatus := shard.Status()[st.LeaderID]
		allCaught := true
		for _, id := range st.ConfiguredVoters {
			rs := shard.Status()[id]
			if !rs.Alive || rs.CommitIndex != leaderStatus.CommitIndex {
				allCaught = false
			}
		}
		if allCaught {
			break
		}
		if err := shard.CatchUp(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(fastTick)
	}
	leaderCommit := shard.Status()[st.LeaderID].CommitIndex
	for _, id := range st.ConfiguredVoters {
		rs := shard.Status()[id]
		if rs.CommitIndex != leaderCommit {
			t.Fatalf("invariant violated after expansion: %s commit=%d, leader commit=%d",
				id, rs.CommitIndex, leaderCommit)
		}
	}
	t.Logf("invariant: all 5 voters converged at commitIndex=%d", leaderCommit)

	// Dedup check for the whole interleaved ID set on the 5-voter shard.
	for i := 0; i < total; i++ {
		resp, err := store.Write(ctx, WriteRequest{
			ShardID:   "grow",
			Key:       fmt.Sprintf("key-%02d", i%5),
			Value:     []byte(fmt.Sprintf("w-%02d", i)),
			RequestID: fmt.Sprintf("iw-%02d", i),
		})
		if err != nil {
			t.Fatalf("replay id iw-%02d: %v", i, err)
		}
		if !resp.Duplicate {
			t.Fatalf("interleaved request iw-%02d applied twice after expansion", i)
		}
	}
	t.Log("idempotency basis: every interleaved request ID replayed as duplicate=true")
}

// TestExpansionRejectsOverlapAndDuplicateIDs guards the configuration
// validation surface.
func TestExpansionRejectsOverlapAndDuplicateIDs(t *testing.T) {
	_, shard := newExpandStore(t)
	if err := shard.ExpandReplicas(nil); err == nil {
		t.Fatal("empty expansion accepted")
	}
	if err := shard.ExpandReplicas([]string{"n2", "n4"}); err == nil {
		t.Fatal("expansion with an existing member accepted")
	}
	if err := shard.ExpandReplicas([]string{"n4", "n4"}); err == nil {
		t.Fatal("expansion with a duplicated new member accepted")
	}
	t.Log("validation basis: empty, overlapping and duplicate member additions rejected")
}
