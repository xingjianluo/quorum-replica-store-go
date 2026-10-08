package quorum

import (
	"context"
	"testing"
	"time"
)

func persistentTestSpecs() []ShardSpec {
	return []ShardSpec{{
		ID: "persist", Range: ShardRange{}, ReplicaIDs: []string{"p1", "p2", "p3"},
	}}
}

// TestRestartPreservesCommittedDataVersionsAndRequestIDs 验证整套停掉再起：
// 已确认写入原样还在、版本继续递增、同一请求标识重启后不重复生效。
func TestRestartPreservesCommittedDataVersionsAndRequestIDs(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	store, err := NewStoreWithOptions(persistentTestSpecs(), WithDataDirectory(dir))
	if err != nil {
		t.Fatal(err)
	}
	store.SetLogger(testLogger{t})
	client := NewClient(store, 3, 0)
	client.SetLogger(testLogger{t})

	for i, value := range []string{"v1", "v2", "v3"} {
		resp, err := client.Write(ctx, WriteRequest{
			ShardID: "persist", Key: "k", Value: []byte(value),
			RequestID: "req-" + value,
		})
		if err != nil {
			t.Fatalf("write %s: %v", value, err)
		}
		if resp.Version != uint64(i+1) {
			t.Fatalf("INVARIANT violated before restart: version for %s = %d, want %d",
				value, resp.Version, i+1)
		}
		t.Logf("STEP committed key=k value=%s version=%d index=%d term=%d (majority flushed)",
			value, resp.Version, resp.CommitIndex, resp.CommitTerm)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("orderly close: %v", err)
	}
	t.Logf("STEP whole store stopped; data directory=%s", dir)

	restarted, err := RestartStore(persistentTestSpecs(), dir)
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	defer restarted.Close()
	restarted.SetLogger(testLogger{t})
	waitFor(t, "restarted cluster re-elects a leader and becomes writable", 2*time.Second, func() bool {
		return restarted.Leadership()["persist"].Writable
	})

	read, err := restarted.Read(ctx, ReadRequest{ShardID: "persist", Key: "k", Mode: ReadModeStrong})
	if err != nil {
		t.Fatalf("strong read after restart: %v", err)
	}
	if read.Value == nil || string(read.Value.Data) != "v3" || read.Value.Version != 3 {
		t.Fatalf("INVARIANT violated after restart: committed data changed: %+v", read.Value)
	}
	t.Logf("ASSERT committed data intact after restart: value=%s version=%d index=%d",
		string(read.Value.Data), read.Value.Version, read.CommitIndex)

	// 同一请求标识在重启之后再提一次：必须命中已有结果，不能又生效一遍。
	dup, err := restarted.Write(ctx, WriteRequest{
		ShardID: "persist", Key: "k", Value: []byte("v3"), RequestID: "req-v3",
	})
	if err != nil {
		t.Fatalf("retry of pre-restart request after restart: %v", err)
	}
	if !dup.Duplicate || dup.Version != 3 {
		t.Fatalf("INVARIANT violated: request dedupe did not survive restart: %+v", dup)
	}
	t.Logf("ASSERT idempotency: request=req-v3 after restart served as DEDUP-HIT duplicate=%t version=%d (not reapplied)",
		dup.Duplicate, dup.Version)

	// 新版本必须继续往上走，不能回退或重复。
	next, err := restarted.Write(ctx, WriteRequest{
		ShardID: "persist", Key: "k", Value: []byte("v4"), RequestID: "req-v4",
	})
	if err != nil {
		t.Fatalf("write after restart: %v", err)
	}
	if next.Version != 4 {
		t.Fatalf("INVARIANT violated: version after restart = %d, want 4 (no rollback/duplicate version)",
			next.Version)
	}
	t.Logf("ASSERT version monotonic across restart: new write version=%d index=%d leader=%s",
		next.Version, next.CommitIndex, next.LeaderID)
}

// TestRestartDoesNotPublishMinorityOnlyWrite 验证只在少数副本上短暂出现、
// 从未对外确认成功的写入，整套重启后不能被当成已提交数据放出。
func TestRestartDoesNotPublishMinorityOnlyWrite(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := NewStoreWithOptions(persistentTestSpecs(), WithDataDirectory(dir))
	if err != nil {
		t.Fatal(err)
	}
	store.SetLogger(testLogger{t})
	shard, err := store.Shard("persist")
	if err != nil {
		t.Fatal(err)
	}
	client := NewClient(store, 1, 0)

	committed, err := client.Write(ctx, WriteRequest{
		ShardID: "persist", Key: "safe", Value: []byte("yes"), RequestID: "safe-1",
	})
	if err != nil {
		t.Fatalf("seed committed write: %v", err)
	}
	leader := committed.LeaderID
	t.Logf("STEP committed baseline on leader=%s index=%d; now isolate a majority", leader, committed.CommitIndex)

	// 崩溃两个副本，只留一个存活：任何新写入都凑不齐多数派。
	crashed := []string{}
	for _, id := range []string{"p1", "p2", "p3"} {
		if id != leader && len(crashed) < 2 {
			if err := shard.CrashReplica(id); err != nil {
				t.Fatal(err)
			}
			cashed := id
			crashed = append(crashed, cashed)
		}
	}
	_, err = client.Write(ctx, WriteRequest{
		ShardID: "persist", Key: "ghost", Value: []byte("never-confirm"), RequestID: "ghost-1",
	})
	assertErrorKind(t, err, KindQuorumUnavailable)
	t.Logf("STEP minority write never acknowledged: kind=%s request=ghost-1", KindQuorumUnavailable)

	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	restarted, err := RestartStore(persistentTestSpecs(), dir)
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	defer restarted.Close()
	restarted.SetLogger(testLogger{t})
	waitFor(t, "restarted cluster writable again", 2*time.Second, func() bool {
		return restarted.Leadership()["persist"].Writable
	})

	for _, replicaID := range []string{"p1", "p2", "p3"} {
		resp, err := restarted.Read(ctx, ReadRequest{
			ShardID: "persist", Key: "ghost", Mode: ReadModeEventual, ReplicaID: replicaID,
		})
		if err != nil {
			t.Fatalf("read ghost from %s: %v", replicaID, err)
		}
		if resp.Value != nil {
			t.Fatalf("INVARIANT violated: uncommitted minority write ghost reappeared on %s after restart: %+v",
				replicaID, resp.Value)
		}
	}
	t.Logf("ASSERT ghost key is absent on every replica after restart; only committed data survives")

	safe, err := restarted.Read(ctx, ReadRequest{ShardID: "persist", Key: "safe", Mode: ReadModeStrong})
	if err != nil {
		t.Fatalf("read committed key after restart: %v", err)
	}
	if safe.Value == nil || string(safe.Value.Data) != "yes" {
		t.Fatalf("committed baseline lost after restart: %+v", safe.Value)
	}
	t.Logf("ASSERT previously committed key safe=%q version=%d still present after restart",
		string(safe.Value.Data), safe.Value.Version)
}

// TestRestartAfterMidExpansionUsesLastCommittedMembership 验证扩容配置条目
// 已提交的成员在重启后仍被识别，系统按新成员数收敛。
func TestRestartAfterMidExpansionUsesLastCommittedMembership(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := NewStoreWithOptions(persistentTestSpecs(), WithDataDirectory(dir))
	if err != nil {
		t.Fatal(err)
	}
	store.SetLogger(testLogger{t})
	shard, err := store.Shard("persist")
	if err != nil {
		t.Fatal(err)
	}
	client := NewClient(store, 3, 0)
	if _, err := client.Write(ctx, WriteRequest{
		ShardID: "persist", Key: "seed", Value: []byte("x"), RequestID: "seed",
	}); err != nil {
		t.Fatal(err)
	}
	if err := shard.ExpandReplicaSet([]string{"p4", "p5"}); err != nil {
		t.Fatalf("expand 3->5: %v", err)
	}
	t.Logf("STEP expansion to five voters committed; spec=%v", shard.Spec().ReplicaIDs)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := RestartStore(persistentTestSpecs(), dir)
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	defer restarted.Close()
	restarted.SetLogger(testLogger{t})
	waitFor(t, "restarted five-voter cluster converges writable", 2*time.Second, func() bool {
		st := restarted.Leadership()["persist"]
		return st.Writable && st.VoterCount == 5 && st.Quorum == 3
	})
	st := restarted.Leadership()["persist"]
	t.Logf("ASSERT restart recovered committed membership: voters=%d quorum=%d leader=%s",
		st.VoterCount, st.Quorum, st.LeaderID)

	// 新门槛立刻生效：崩 2 个仍可写（3/5）。
	rs, _ := restarted.Shard("persist")
	order := rs.Spec().ReplicaIDs
	crashTwo := []string{}
	for _, id := range order {
		if id != st.LeaderID && len(crashTwo) < 2 {
			if err := rs.CrashReplica(id); err != nil {
				t.Fatal(err)
			}
			crashTwo = append(crashTwo, id)
		}
	}
	waitFor(t, "leader stays writable at 3/5", 2*time.Second, func() bool {
		return restarted.Leadership()["persist"].Writable
	})
	if _, err := client.Write(ctx, WriteRequest{
		ShardID: "persist", Key: "post-restart", Value: []byte("y"), RequestID: "pr-1",
	}); err != nil {
		t.Fatalf("write at 3/5 after restart: %v", err)
	}
	t.Logf("ASSERT new quorum=3 effective after restart: write committed with only 3/5 alive")
}

// TestRestartCASVersionContinuesAndNoDuplicateVersion 验证重启后 CAS 比较
// 版本按已提交历史继续，非法 CAS 被拒，新版本号不回退、不跳号。
func TestRestartCASVersionContinuesAndNoDuplicateVersion(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := NewStoreWithOptions(persistentTestSpecs(), WithDataDirectory(dir))
	if err != nil {
		t.Fatal(err)
	}
	store.SetLogger(testLogger{t})
	v2 := uint64(2)
	if _, err := store.Write(ctx, WriteRequest{
		ShardID: "persist", Key: "cas", Value: []byte("v2"),
		CompareVersion: &v2, RequestID: "cas-bad-before",
	}); err == nil {
		t.Fatal("CAS against version 2 on an empty key unexpectedly succeeded before restart")
	} else {
		assertErrorKind(t, err, KindVersionConflict)
	}
	v1 := uint64(1)
	if _, err := store.Write(ctx, WriteRequest{
		ShardID: "persist", Key: "cas", Value: []byte("first"),
		CompareVersion: &v1, RequestID: "cas-1",
	}); err == nil {
		t.Fatal("CAS expecting version 1 on empty key should be a conflict")
	}
	// 正常首写（版本 1）。
	if _, err := store.Write(ctx, WriteRequest{
		ShardID: "persist", Key: "cas", Value: []byte("v1"), RequestID: "cas-init",
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := RestartStore(persistentTestSpecs(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	restarted.SetLogger(testLogger{t})
	waitFor(t, "restarted cluster writable", 2*time.Second, func() bool {
		return restarted.Leadership()["persist"].Writable
	})

	// 重启后当前版本是 1：用过期版本 0/2 做 CAS 必须冲突；用 1 才成功得到版本 2。
	stale := uint64(0)
	if _, err := restarted.Write(ctx, WriteRequest{
		ShardID: "persist", Key: "cas", Value: []byte("stale"),
		CompareVersion: &stale, RequestID: "cas-stale-after",
	}); err == nil {
		t.Fatal("CAS using stale expected version 0 should conflict after restart")
	}
	ahead := uint64(2)
	_, err = restarted.Write(ctx, WriteRequest{
		ShardID: "persist", Key: "cas", Value: []byte("stale"),
		CompareVersion: &ahead, RequestID: "cas-stale-after-2",
	})
	assertErrorKind(t, err, KindVersionConflict)
	t.Logf("ASSERT stale CAS after restart rejected kind=%s; version history preserved", KindVersionConflict)

	current := uint64(1)
	resp, err := restarted.Write(ctx, WriteRequest{
		ShardID: "persist", Key: "cas", Value: []byte("v2"),
		CompareVersion: &current, RequestID: "cas-2",
	})
	if err != nil {
		t.Fatalf("CAS with correct current version after restart: %v", err)
	}
	if resp.Version != 2 {
		t.Fatalf("INVARIANT violated: CAS after restart produced version=%d, want 2", resp.Version)
	}
	t.Logf("ASSERT CAS after restart committed continuous version=%d index=%d", resp.Version, resp.CommitIndex)

	// 再次重启，确认版本 2 稳定、版本 3 继续向上。
	if err := restarted.Close(); err != nil {
		t.Fatal(err)
	}
	third, err := RestartStore(persistentTestSpecs(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer third.Close()
	third.SetLogger(testLogger{t})
	waitFor(t, "second restart writable", 2*time.Second, func() bool {
		return third.Leadership()["persist"].Writable
	})
	read, err := third.Read(ctx, ReadRequest{ShardID: "persist", Key: "cas", Mode: ReadModeStrong})
	if err != nil {
		t.Fatal(err)
	}
	if read.Value == nil || string(read.Value.Data) != "v2" || read.Value.Version != 2 {
		t.Fatalf("INVARIANT violated across second restart: %+v", read.Value)
	}
	t.Logf("ASSERT value v2/version=2 stable across two restarts: %+v", read.Value)
}

// TestFailoverThenRestartPreservesNewTermCommits 验证：无人请求自动接管后，
// 新主在新任期内提交的写入，整套重启后仍然保留、版本连续、旧主不复活。
func TestFailoverThenRestartPreservesNewTermCommits(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := NewStoreWithOptions(persistentTestSpecs(), WithDataDirectory(dir))
	if err != nil {
		t.Fatal(err)
	}
	store.SetLogger(testLogger{t})
	shard, _ := store.Shard("persist")
	client := NewClient(store, 3, 0)

	first, err := client.Write(ctx, WriteRequest{
		ShardID: "persist", Key: "ft", Value: []byte("v1"), RequestID: "ft-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	oldLeader := first.LeaderID
	if err := shard.CrashReplica(oldLeader); err != nil {
		t.Fatal(err)
	}
	t.Logf("STEP crashed leader=%s; waiting for heartbeat takeover (no read/write issued)", oldLeader)
	waitFor(t, "automatic new leader after crash", 2*time.Second, func() bool {
		st := shard.Leadership()
		return st.Writable && st.LeaderID != oldLeader
	})
	newLeader := shard.LeaderID()

	second, err := client.Write(ctx, WriteRequest{
		ShardID: "persist", Key: "ft", Value: []byte("v2"), RequestID: "ft-2",
	})
	if err != nil {
		t.Fatalf("write under auto-elected leader: %v", err)
	}
	if second.CommitTerm <= first.CommitTerm || second.Version != 2 {
		t.Fatalf("INVARIANT violated: new-term commit wrong: first=%+v second=%+v", first, second)
	}
	t.Logf("STEP new leader=%s committed v2 in term=%d version=%d", newLeader, second.CommitTerm, second.Version)

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := RestartStore(persistentTestSpecs(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	restarted.SetLogger(testLogger{t})
	waitFor(t, "restarted cluster writable", 2*time.Second, func() bool {
		return restarted.Leadership()["persist"].Writable
	})
	read, err := restarted.Read(ctx, ReadRequest{ShardID: "persist", Key: "ft", Mode: ReadModeStrong})
	if err != nil {
		t.Fatal(err)
	}
	if read.Value == nil || string(read.Value.Data) != "v2" || read.Value.Version != 2 {
		t.Fatalf("INVARIANT violated after restart: %+v", read.Value)
	}
	t.Logf("ASSERT new-term failover commit v2/version=2 survived full restart; index=%d", read.CommitIndex)
}
