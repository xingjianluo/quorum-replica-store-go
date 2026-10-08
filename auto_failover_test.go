package quorum

import (
	"context"
	"testing"
	"time"
)

// waitFor 反复检查条件直到成立或超时；每一步判定都通过 t.Logf 输出依据。
func waitFor(t *testing.T, what string, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			t.Logf("ASSERT %s: satisfied at %s", what, time.Now().Format("15:04:05.000"))
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("ASSERT %s: not satisfied within %s", what, timeout)
}

func waitForLeader(t *testing.T, shard *ReplicaSet, timeout time.Duration) string {
	t.Helper()
	var leaderID string
	waitFor(t, "a leader is elected without any client request", timeout, func() bool {
		leaderID = shard.LeaderID()
		return leaderID != ""
	})
	return leaderID
}

// TestAutoFailoverElectsNewLeaderWithoutClientRequests 验证：主副本失联后，
// 在没有任何读写请求的情况下，后台心跳在选举超时内自动选出新主。
func TestAutoFailoverElectsNewLeaderWithoutClientRequests(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	client := NewClient(store, 1, 0)
	shard, err := store.Shard("users")
	if err != nil {
		t.Fatal(err)
	}

	first, err := client.Write(ctx, WriteRequest{
		ShardID: "users", Key: "auto", Value: []byte("v1"), RequestID: "auto-1",
	})
	if err != nil {
		t.Fatalf("seed write: %v", err)
	}
	oldLeader := first.LeaderID
	t.Logf("STEP initial leader=%s established by a normal write term=%d", oldLeader, first.CommitTerm)

	if err := shard.CrashReplica(oldLeader); err != nil {
		t.Fatal(err)
	}
	t.Logf("STEP leader=%s crashed at %s; no further client requests will be issued until after the new leader exists",
		oldLeader, time.Now().Format("15:04:05.000"))

	start := time.Now()
	var newLeader string
	waitFor(t, "heartbeat loop elects a new leader after the old one disappeared", 2*time.Second, func() bool {
		status := shard.Leadership()
		newLeader = status.LeaderID
		return status.Writable && newLeader != "" && newLeader != oldLeader
	})
	elapsed := time.Now().Sub(start)
	t.Logf("ASSERT automatic takeover took %s (electionTimeout default=200ms); newLeader=%s", elapsed, newLeader)
	if elapsed > 2*time.Second {
		t.Fatalf("takeover was not within the expected second-level window: %s", elapsed)
	}

	// 接管完成后首次客户端操作必须直接走新主，且版本连续。
	second, err := client.Write(ctx, WriteRequest{
		ShardID: "users", Key: "auto", Value: []byte("v2"), RequestID: "auto-2",
	})
	if err != nil {
		t.Fatalf("write after automatic takeover: %v", err)
	}
	if second.LeaderID == oldLeader {
		t.Fatalf("INVARIANT violated: write after takeover was served by the crashed old leader %s", oldLeader)
	}
	if second.Version != 2 || second.CommitTerm <= first.CommitTerm {
		t.Fatalf("INVARIANT violated: revision/term did not advance after takeover: first=%+v second=%+v",
			first, second)
	}
	t.Logf("ASSERT post-takeover write served by leader=%s version=%d term=%d (dedup: new commit, not duplicate)",
		second.LeaderID, second.Version, second.CommitTerm)
}

// TestLeadershipReportsReasonsHonestly 验证诊断接口如实说明：可写、主被
// 分区、多数派不足三种状态，且少数派存活时绝不能宣称旧主仍在服务。
func TestLeadershipReportsReasonsHonestly(t *testing.T) {
	store := testStore(t)
	shard, err := store.Shard("users")
	if err != nil {
		t.Fatal(err)
	}
	leader := waitForLeader(t, shard, 2*time.Second)
	t.Logf("STEP healthy cluster, elected leader=%s", leader)

	status := shard.Leadership()
	if !status.Writable || status.LeaderID != leader {
		t.Fatalf("healthy leadership status: %+v", status)
	}
	if status.VoterCount != 3 || status.Quorum != 2 || status.AliveVoters != 3 {
		t.Fatalf("quorum accounting wrong: %+v", status)
	}
	t.Logf("ASSERT writable=true voters=%d quorum=%d reachable=%d leader=%s",
		status.VoterCount, status.Quorum, status.LeaderReachable, status.LeaderID)

	var isolated, peer1, peer2 string
	if leader == "r1" {
		isolated, peer1, peer2 = "r1", "r2", "r3"
	} else if leader == "r2" {
		isolated, peer1, peer2 = "r2", "r1", "r3"
	} else {
		isolated, peer1, peer2 = "r3", "r1", "r2"
	}
	if err := shard.AddPartition(isolated, peer1); err != nil {
		t.Fatal(err)
	}
	if err := shard.AddPartition(isolated, peer2); err != nil {
		t.Fatal(err)
	}
	t.Logf("STEP partitioned leader=%s from the other two replicas", isolated)

	waitFor(t, "majority side elects a replacement leader automatically", 2*time.Second, func() bool {
		s := shard.Leadership()
		return s.Writable && s.LeaderID != isolated
	})
	t.Logf("ASSERT old partitioned leader=%s is no longer reported as serving; new leader=%s",
		isolated, shard.LeaderID())

	// 现在让多数派一侧的两个副本崩溃，只剩被隔离的旧主存活：凑不出多数派，
	// 必须明确不可写，且原因是缺多数派，不能宣称旧主在服务。
	if err := shard.CrashReplica(peer1); err != nil {
		t.Fatal(err)
	}
	if err := shard.CrashReplica(peer2); err != nil {
		t.Fatal(err)
	}
	t.Logf("STEP crashed majority peers=%s,%s; only isolated old leader=%s remains alive", peer1, peer2, isolated)
	waitFor(t, "status converges to quorum-unavailable", 2*time.Second, func() bool {
		s := shard.Leadership()
		return !s.Writable && s.Reason == KindQuorumUnavailable && s.AliveVoters == 1
	})
	s := shard.Leadership()
	t.Logf("ASSERT writable=false reason=%s aliveVoters=%d/%d quorum=%d leaderField=%q (must not claim old leader serves)",
		s.Reason, s.AliveVoters, s.VoterCount, s.Quorum, s.LeaderID)

	// 写入必须被拒，不能悄悄降级。
	_, err = store.Write(context.Background(), WriteRequest{
		ShardID: "users", Key: "auto", Value: []byte("must-not-commit"), RequestID: "minority-only",
	})
	assertErrorKind(t, err, KindQuorumUnavailable)
	t.Logf("ASSERT write with no majority rejected kind=%s (no degraded commit)", KindQuorumUnavailable)
}

// TestElectionTimeoutBoundsTakeover 用自定义节拍验证接管时间上界可预期：
// heartbeat=10ms、timeout=80ms 时，主崩溃后接管应远在 1s 内完成。
func TestElectionTimeoutBoundsTakeover(t *testing.T) {
	store, err := NewStoreWithOptions([]ShardSpec{{
		ID: "s", Range: ShardRange{}, ReplicaIDs: []string{"a", "b", "c"},
	}}, WithFailoverTiming(10*time.Millisecond, 80*time.Millisecond, true))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	store.SetLogger(testLogger{t})
	shard, err := store.Shard("s")
	if err != nil {
		t.Fatal(err)
	}
	leader := waitForLeader(t, shard, time.Second)

	start := time.Now()
	if err := shard.CrashReplica(leader); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "new leader elected within tight timing bound", time.Second, func() bool {
		st := shard.Leadership()
		return st.Writable && st.LeaderID != leader
	})
	elapsed := time.Since(start)
	t.Logf("ASSERT takeover under heartbeat=10ms timeout=80ms took %s", elapsed)
	if elapsed > time.Second {
		t.Fatalf("takeover %s exceeded the configured bound", elapsed)
	}
}
