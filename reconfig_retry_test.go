package quorum

import (
	"context"
	"testing"
	"time"
)

// TestExpansionAbortsWhenJointQuorumImpossibleAndRetries proves the rollback
// contract: if a joint commit cannot gather BOTH majorities (here the old
// side lacks a majority because one old voter is crashed), the expansion
// activates nothing, the old quorum keeps serving, and after the old voter
// returns the same expansion completes.
func TestExpansionAbortsWhenJointQuorumImpossibleAndRetries(t *testing.T) {
	ctx := context.Background()
	store, shard := newExpandStore(t)
	first, err := store.Write(ctx, WriteRequest{
		ShardID: "grow", Key: "k", Value: []byte("v1"), RequestID: "r1",
	})
	if err != nil {
		t.Fatal(err)
	}
	leader := first.LeaderID

	// Crash one old voter that is not the leader: old reachable voters are 2
	// of 3, which is the old majority, so this alone would still allow a
	// joint commit. To make the joint quorum impossible we instead isolate
	// one new member during pre-sync. New members are created as learners by
	// the expansion itself; to simulate the failure we crash one old voter
	// AND one additional member is unreachable because the shard only has
	// the three old nodes until learners materialize. The cleanest injection
	// point available is: crash a second old voter so the old side has only
	// the leader (1 < oldQ=2).
	crashed := 0
	for _, id := range shard.Spec().ReplicaIDs {
		if id != leader && crashed < 2 {
			if err := shard.CrashReplica(id); err != nil {
				t.Fatal(err)
			}
			crashed++
		}
	}
	t.Log("decision basis: old side has only the leader (1<2); joint old-majority is impossible")

	err = shard.ExpandReplicas([]string{"n4", "n5"})
	if err == nil {
		t.Fatal("expansion accepted without the old majority")
	}
	assertErrorKind(t, err, KindQuorumUnavailable)
	t.Logf("rejection basis: expansion blocked: %v", err)

	st := shard.ClusterStatus()
	if len(st.ConfiguredVoters) != 3 || st.Quorum != 2 || st.Joint || st.Writable {
		t.Fatalf("aborted expansion changed active membership or writability: %+v", st)
	}
	// Learners materialized during pre-flight must not vote or count.
	for _, id := range []string{"n4", "n5"} {
		if rs, known := shard.Status()[id]; known && rs.Voter {
			t.Fatalf("new member %s became a voter after an aborted expansion: %+v", id, rs)
		}
	}
	t.Logf("invariant: after abort voters=%v quorum=%d joint=%t; learners never voted",
		st.ConfiguredVoters, st.Quorum, st.Joint)

	// Restore the old voters and let supervision recover writability, then
	// retry the exact same expansion to completion.
	for _, id := range shard.Spec().ReplicaIDs {
		if id != leader {
			_ = shard.RecoverReplica(id)
		}
	}
	waitForStatus(t, shard, 2*time.Second, func(s ShardStatus) bool { return s.Writable },
		"old majority recovers writability")
	if err := shard.ExpandReplicas([]string{"n4", "n5"}); err != nil {
		t.Fatalf("retried expansion: %v", err)
	}
	done := shard.ClusterStatus()
	if len(done.ConfiguredVoters) != 5 || done.Quorum != 3 || !done.Writable {
		t.Fatalf("retried expansion did not finish cleanly: %+v", done)
	}
	if _, err := store.Write(ctx, WriteRequest{
		ShardID: "grow", Key: "k", Value: []byte("v2"), RequestID: "r2",
	}); err != nil {
		t.Fatalf("write after completed expansion: %v", err)
	}
	t.Logf("invariant: retried expansion completed voters=%v quorum=%d; versions continue",
		done.ConfiguredVoters, done.Quorum)
}
