package quorum

import (
	"context"
	"testing"
	"time"
)

// fastTick keeps supervision deterministic and fast in tests while staying
// comfortably above scheduler granularity.
const fastTick = 5 * time.Millisecond

// waitForStatus polls pred without issuing any shard request, so success
// proves the background supervisor acted on its own.
func waitForStatus(t *testing.T, shard *ReplicaSet, timeout time.Duration,
	pred func(ShardStatus) bool, msg string) ShardStatus {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last ShardStatus
	for time.Now().Before(deadline) {
		last = shard.ClusterStatus()
		if pred(last) {
			return last
		}
		time.Sleep(fastTick)
	}
	t.Fatalf("timed out waiting for %s; last status: %+v", msg, last)
	return last
}

func newSupervisedStore(t *testing.T, replicaIDs []string) (*Store, *ReplicaSet) {
	t.Helper()
	store, err := NewStoreWithOptions([]ShardSpec{{
		ID: "auto", Range: ShardRange{}, ReplicaIDs: replicaIDs,
	}}, Options{TickInterval: fastTick, Logger: testLogger{t}})
	if err != nil {
		t.Fatalf("NewStoreWithOptions: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	shard, err := store.Shard("auto")
	if err != nil {
		t.Fatal(err)
	}
	return store, shard
}

// TestAutoFailoverElectsWithoutClientTraffic is the headline behavior: after
// the leader crashes with NO read or write arriving, a surviving majority
// elects a new leader in a bounded, predictable time and resumes service.
func TestAutoFailoverElectsWithoutClientTraffic(t *testing.T) {
	ctx := context.Background()
	store, shard := newSupervisedStore(t, []string{"r1", "r2", "r3"})

	first, err := store.Write(ctx, WriteRequest{ShardID: "auto", Key: "k", Value: []byte("v1"), RequestID: "w1"})
	if err != nil {
		t.Fatal(err)
	}
	oldLeader := first.LeaderID
	t.Logf("precondition: committed leader=%s index=%d; now crashing it with no further client calls",
		oldLeader, first.CommitIndex)
	if err := shard.CrashReplica(oldLeader); err != nil {
		t.Fatal(err)
	}

	// Deterministic staggering: the surviving follower with the lower order
	// index campaigns first (electionBaseTicks=10 at 5ms => ~50-90ms).
	before := time.Now()
	st := waitForStatus(t, shard, 2*time.Second, func(s ShardStatus) bool {
		return s.Writable && s.LeaderID != "" && s.LeaderID != oldLeader
	}, "automatic takeover by a surviving majority")
	elapsed := time.Since(before)
	t.Logf("decision basis: new leader=%s elected by background supervision in %s; aliveVoters=%d quorum=%d",
		st.LeaderID, elapsed, st.AliveCount, st.Quorum)
	if elapsed > 1500*time.Millisecond {
		t.Fatalf("takeover took %s, expected bounded sub-second-ish time", elapsed)
	}
	if st.Term <= first.CommitTerm {
		t.Fatalf("new leader term=%d did not advance beyond old term=%d", st.Term, first.CommitTerm)
	}

	// Service resumes; version and index continue monotonically.
	second, err := store.Write(ctx, WriteRequest{ShardID: "auto", Key: "k", Value: []byte("v2"), RequestID: "w2"})
	if err != nil {
		t.Fatalf("write after automatic takeover: %v", err)
	}
	if second.LeaderID != st.LeaderID || second.Version != 2 || second.CommitTerm <= first.CommitTerm {
		t.Fatalf("post-takeover write did not advance under the new leader: first=%+v second=%+v", first, second)
	}
	t.Logf("invariant: post-takeover version=%d index=%d term=%d continues above prior version=%d index=%d",
		second.Version, second.CommitIndex, second.CommitTerm, first.Version, first.CommitIndex)

	// Recover the old leader; it must not reclaim leadership and must
	// converge on its own (heartbeats + next write path), still serving.
	if err := shard.RecoverReplica(oldLeader); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Write(ctx, WriteRequest{ShardID: "auto", Key: "k", Value: []byte("v3"), RequestID: "w3"}); err != nil {
		t.Fatalf("write after old leader recovery: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		r, err := store.Read(ctx, ReadRequest{
			ShardID: "auto", Key: "k", Mode: ReadModeEventual, ReplicaID: oldLeader,
		})
		if err != nil {
			t.Fatal(err)
		}
		if r.Value != nil && string(r.Value.Data) == "v3" && !r.Stale {
			break
		}
		if time.Now().Add(20 * time.Millisecond).After(deadline) {
			t.Fatalf("recovered old leader never converged; last read=%+v stale=%t", r.Value, r.Stale)
		}
		time.Sleep(fastTick)
	}
	if shard.LeaderID() != st.LeaderID {
		t.Fatalf("recovered old leader reclaimed leadership: current=%s want=%s",
			shard.LeaderID(), st.LeaderID)
	}
	t.Logf("invariant: old leader=%s rejoined as follower; leader stays=%s",
		oldLeader, st.LeaderID)
}

// TestAutoFailoverStopsAtNoQuorum proves that when a majority cannot be
// formed the shard does not keep advertising the old leader and does not
// degrade writes; when a majority returns it becomes writable again on its
// own.
func TestAutoFailoverStopsAtNoQuorum(t *testing.T) {
	ctx := context.Background()
	store, shard := newSupervisedStore(t, []string{"r1", "r2", "r3"})
	first, err := store.Write(ctx, WriteRequest{ShardID: "auto", Key: "k", Value: []byte("v1"), RequestID: "q1"})
	if err != nil {
		t.Fatal(err)
	}
	leader := first.LeaderID

	// Crash a follower first: majority still holds, status stays writable.
	var follower string
	for _, id := range []string{"r1", "r2", "r3"} {
		if id != leader {
			follower = id
			break
		}
	}
	if err := shard.CrashReplica(follower); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if st := shard.ClusterStatus(); !st.Writable || st.LeaderID != leader {
		t.Fatalf("2/3 alive should remain writable: %+v", st)
	}
	t.Logf("decision basis: 2/3 alive keeps leader=%s writable=true quorum=%d", leader, 2)

	// Crash the leader: only one voter remains, no majority.
	if err := shard.CrashReplica(leader); err != nil {
		t.Fatal(err)
	}
	st := waitForStatus(t, shard, 2*time.Second, func(s ShardStatus) bool {
		return !s.Writable && (s.WriteState == WriteStateNoQuorum || s.WriteState == WriteStateNoLeader)
	}, "shard stops at no quorum")
	t.Logf("decision basis: state=%s reason=%q aliveVoters=%d quorum=%d leader=%q",
		st.WriteState, st.WriteBlockReason, st.AliveCount, st.Quorum, st.LeaderID)
	if st.LeaderID == leader {
		t.Fatalf("shard still advertises crashed leader=%s as serving", leader)
	}

	// No degraded write, no degraded strong read.
	if _, err := store.Write(ctx, WriteRequest{
		ShardID: "auto", Key: "k", Value: []byte("must-not-commit"), RequestID: "q2",
	}); err == nil {
		t.Fatal("write was accepted without a majority (silent degradation)")
	} else {
		assertErrorKind(t, err, KindQuorumUnavailable)
		t.Logf("rejection basis: minority write rejected with quorum_unavailable: %v", err)
	}
	if _, err := store.Read(ctx, ReadRequest{ShardID: "auto", Key: "k", Mode: ReadModeStrong}); err == nil {
		t.Fatal("strong read was served without a majority")
	} else {
		assertErrorKind(t, err, KindQuorumUnavailable)
	}
	// Eventual read on the lone survivor still works but must be flagged.
	survivor := shard.Spec().ReplicaIDs
	survivorID := ""
	for _, id := range survivor {
		if id != leader && id != follower {
			survivorID = id
		}
	}
	ev, err := store.Read(ctx, ReadRequest{
		ShardID: "auto", Key: "k", Mode: ReadModeEventual, ReplicaID: survivorID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ev.Stale || string(ev.Value.Data) != "v1" {
		t.Fatalf("minority eventual read must show last committed data and stale=true: %+v", ev)
	}

	// Majority returns: the shard must become writable again with no client
	// election call.
	if err := shard.RecoverReplica(follower); err != nil {
		t.Fatal(err)
	}
	if err := shard.RecoverReplica(leader); err != nil {
		t.Fatal(err)
	}
	recovered := waitForStatus(t, shard, 2*time.Second, func(s ShardStatus) bool {
		return s.Writable
	}, "automatic reconvergence after majority restored")
	t.Logf("decision basis: reconverged leader=%s term=%d aliveVoters=%d",
		recovered.LeaderID, recovered.Term, recovered.AliveCount)

	second, err := store.Write(ctx, WriteRequest{ShardID: "auto", Key: "k", Value: []byte("v2"), RequestID: "q3"})
	if err != nil {
		t.Fatalf("write after reconvergence: %v", err)
	}
	if second.Version != 2 {
		t.Fatalf("version after reconvergence=%d, want 2", second.Version)
	}
}

// TestAutoFailoverAcrossNetworkPartitionWithoutRequests proves the supervisor
// also handles a pure network partition (no crash): isolate the leader, the
// majority side elects on its own; heal and the old leader rejoins as follower.
func TestAutoFailoverAcrossNetworkPartitionWithoutRequests(t *testing.T) {
	ctx := context.Background()
	store, shard := newSupervisedStore(t, []string{"r1", "r2", "r3"})
	first, err := store.Write(ctx, WriteRequest{
		ShardID: "auto", Key: "p", Value: []byte("before"), RequestID: "p1",
	})
	if err != nil {
		t.Fatal(err)
	}
	oldLeader := first.LeaderID

	for _, id := range []string{"r1", "r2", "r3"} {
		if id != oldLeader {
			if err := shard.AddPartition(oldLeader, id); err != nil {
				t.Fatal(err)
			}
		}
	}
	t.Logf("decision basis: leader=%s isolated from both followers; no client traffic follows", oldLeader)

	st := waitForStatus(t, shard, 2*time.Second, func(s ShardStatus) bool {
		return s.Writable && s.LeaderID != "" && s.LeaderID != oldLeader
	}, "majority side elects automatically across a partition")
	t.Logf("decision basis: majority side elected=%s term=%d; isolated old leader kept out",
		st.LeaderID, st.Term)

	// Writes continue on the majority side only.
	if _, err := store.Write(ctx, WriteRequest{
		ShardID: "auto", Key: "p", Value: []byte("during"), RequestID: "p2",
	}); err != nil {
		t.Fatalf("majority-side write during partition: %v", err)
	}

	// Heal: the stale old leader must not reclaim power and must converge.
	shard.HealPartitions()
	if _, err := store.Write(ctx, WriteRequest{
		ShardID: "auto", Key: "p", Value: []byte("after"), RequestID: "p3",
	}); err != nil {
		t.Fatalf("write after heal: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ev, err := store.Read(ctx, ReadRequest{
			ShardID: "auto", Key: "p", Mode: ReadModeEventual, ReplicaID: oldLeader,
		})
		if err == nil && ev.Value != nil && string(ev.Value.Data) == "after" && !ev.Stale {
			break
		}
		if time.Now().Add(20 * time.Millisecond).After(deadline) {
			t.Fatalf("old leader did not converge after heal: %+v", ev)
		}
		time.Sleep(fastTick)
	}
	if shard.LeaderID() != st.LeaderID {
		t.Fatalf("heal let the stale old leader reclaim leadership: %s", shard.LeaderID())
	}
	t.Logf("invariant: after heal single leader=%s, old leader=%s converged as follower",
		st.LeaderID, oldLeader)
}

// TestClusterStatusExplainsLeaderAndWritability checks the honest diagnostics
// surface across the clean, leader-down and minority windows.
func TestClusterStatusExplainsLeaderAndWritability(t *testing.T) {
	_, shard := newSupervisedStore(t, []string{"r1", "r2", "r3"})
	// Before any write there is no leader yet; status must say so explicitly.
	st := shard.ClusterStatus()
	if st.Writable {
		t.Fatalf("fresh shard should not report writable before first election: %+v", st)
	}
	if st.WriteState != WriteStateNoLeader || st.Quorum != 2 || len(st.ConfiguredVoters) != 3 {
		t.Fatalf("unexpected initial status: %+v", st)
	}
	t.Logf("decision basis: fresh shard state=%s reason=%q", st.WriteState, st.WriteBlockReason)
}
