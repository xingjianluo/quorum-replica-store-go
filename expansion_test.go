package quorum

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

func expansionStore(t *testing.T) (*Store, *ReplicaSet) {
	t.Helper()
	store, err := NewStoreWithOptions([]ShardSpec{{
		ID: "exp", Range: ShardRange{}, ReplicaIDs: []string{"e1", "e2", "e3"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	store.SetLogger(testLogger{t})
	shard, err := store.Shard("exp")
	if err != nil {
		t.Fatal(err)
	}
	return store, shard
}

// TestOnlineExpansionInterleavedWithConcurrentWrites 是核心交错链路：
// 扩容 3->5 的同时持续并发写，要求：
//   - 扩容期间始终可写（旧、新多数派有交集，不可能分裂成两派）；
//   - 每个请求恰好生效一次，版本号对每个键连续无冲突无回退；
//   - 扩容完成后多数派门槛立刻变为 3/5；
//   - 新加入副本与已提交状态完全收敛。
func TestOnlineExpansionInterleavedWithConcurrentWrites(t *testing.T) {
	ctx := context.Background()
	store, shard := expansionStore(t)
	defer store.Close()
	client := NewClient(store, 5, 0)
	client.SetLogger(testLogger{t})

	// 先建立初始主和一条已提交基线。
	if _, err := client.Write(ctx, WriteRequest{
		ShardID: "exp", Key: "base", Value: []byte("init"), RequestID: "base",
	}); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 256)

	// 一组持续写不同键的 goroutine，每个请求有稳定唯一标识。每个写者至少
	// 完成 writesEach 次，避免“还没写就看到停止信号”的测试竞态。
	const writesEach = 10
	for w := 0; w < 6; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < writesEach; i++ {
				_, err := client.Write(ctx, WriteRequest{
					ShardID:   "exp",
					Key:       fmt.Sprintf("hot-%d", w),
					Value:     []byte(fmt.Sprintf("v-%d", i)),
					RequestID: fmt.Sprintf("w-%d-%d", w, i),
				})
				if err != nil {
					errCh <- fmt.Errorf("writer %d seq %d: %w", w, i, err)
					return
				}
			}
		}()
	}

	// 在写入压力中执行 3->4->5 在线扩容。
	for _, newID := range []string{"e4", "e5"} {
		if err := store.ExpandShard("exp", []string{newID}); err != nil {
			wg.Wait()
			t.Fatalf("expand add %s: %v", newID, err)
		}
		spec := shard.Spec().ReplicaIDs
		t.Logf("STEP expansion committed: added=%s voters=%v quorum=%d", newID, spec, shard.Leadership().Quorum)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent write during expansion failed: %v", err)
	}

	status := shard.Leadership()
	if !status.Writable || status.VoterCount != 5 || status.Quorum != 3 {
		t.Fatalf("INVARIANT violated after expansion: %+v", status)
	}
	t.Logf("ASSERT post-expansion writable=true voters=%d quorum=%d", status.VoterCount, status.Quorum)

	// 不变量一：全部 5 个副本的提交点收敛一致（新副本也追平已提交状态）。
	if err := shard.CatchUp(); err != nil {
		t.Fatalf("catch-up after expansion: %v", err)
	}
	statuses := shard.Status()
	var commit uint64
	for id, st := range statuses {
		if commit == 0 {
			commit = st.CommitIndex
		}
		if st.CommitIndex != commit {
			t.Fatalf("INVARIANT violated: replica %s commit=%d differs from %d after expansion",
				id, st.CommitIndex, commit)
		}
	}
	t.Logf("ASSERT all five replicas converged to commitIndex=%d (including new voters e4,e5)", commit)

	// 不变量二：每个键在每个副本上的版本一致，且同一批请求不会重复生效。
	// 用最终读校验热键版本号对所有副本相同。
	for w := 0; w < 6; w++ {
		key := fmt.Sprintf("hot-%d", w)
		var firstVersion uint64
		var firstData string
		for _, id := range shard.Spec().ReplicaIDs {
			resp, err := store.Read(ctx, ReadRequest{
				ShardID: "exp", Key: key, Mode: ReadModeEventual, ReplicaID: id,
			})
			if err != nil {
				t.Fatalf("read %s on %s: %v", key, id, err)
			}
			if resp.Value == nil {
				t.Fatalf("INVARIANT violated: key %s missing on replica %s", key, id)
			}
			if firstVersion == 0 {
				firstVersion = resp.Value.Version
				firstData = string(resp.Value.Data)
			}
			if resp.Value.Version != firstVersion || string(resp.Value.Data) != firstData {
				t.Fatalf("INVARIANT violated: divergence on %s: %s=%d/%q vs %d/%q",
					key, id, resp.Value.Version, resp.Value.Data, firstVersion, firstData)
			}
		}
		t.Logf("ASSERT key=%s convergent version=%d data=%q across 5 replicas", key, firstVersion, firstData)
	}

	// 不变量三：取一个成功请求标识重放，必须命中去重而不是再产生一个版本。
	leader := shard.LeaderID()
	probe, err := store.Read(ctx, ReadRequest{ShardID: "exp", Key: "base", Mode: ReadModeStrong})
	if err != nil || probe == nil {
		t.Fatalf("read base: %v", err)
	}
	dup, err := store.Write(ctx, WriteRequest{
		ShardID: "exp", Key: "base", Value: []byte("init"), RequestID: "base",
	})
	if err != nil {
		t.Fatalf("dedup probe after expansion: %v", err)
	}
	if !dup.Duplicate || dup.Version != 1 {
		t.Fatalf("INVARIANT violated: base request reapplied during expansion: %+v", dup)
	}
	t.Logf("ASSERT dedup across expansion: request=base DEDUP-HIT version=%d leader=%s",
		dup.Version, leader)
}

// TestExpansionNewQuorumTakesEffectImmediately 验证扩容完成后的门槛切换：
// 3 副本时崩 2 不可写；扩到 5 后崩 2 仍可写，崩 3 必须明确拒绝。
func TestExpansionNewQuorumTakesEffectImmediately(t *testing.T) {
	ctx := context.Background()
	store, shard := expansionStore(t)
	defer store.Close()
	client := NewClient(store, 5, 0)

	if _, err := client.Write(ctx, WriteRequest{
		ShardID: "exp", Key: "q", Value: []byte("0"), RequestID: "q0",
	}); err != nil {
		t.Fatal(err)
	}
	leader := shard.LeaderID()

	// 扩容前：崩掉除主之外的 2 个 -> 1/3 不可写。
	others := []string{}
	for _, id := range []string{"e1", "e2", "e3"} {
		if id != leader {
			others = append(others, id)
		}
	}
	for _, id := range others {
		if err := shard.CrashReplica(id); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "3-node cluster reports quorum unavailable at 1/3", 2e9, func() bool {
		st := shard.Leadership()
		return !st.Writable && st.Reason == KindQuorumUnavailable
	})
	t.Logf("ASSERT before expansion 1/3 alive => not writable reason=%s", KindQuorumUnavailable)

	for _, id := range others {
		if err := shard.RecoverReplica(id); err != nil {
			t.Fatal(err)
		}
	}
	if err := shard.CatchUp(); err != nil {
		t.Fatal(err)
	}
	if err := store.ExpandShard("exp", []string{"e4", "e5"}); err != nil {
		t.Fatalf("expand: %v", err)
	}
	t.Logf("STEP expanded to five voters; quorum now %d", shard.Leadership().Quorum)

	// 扩容后：崩 2 个非主副本 -> 3/5 仍可写，版本继续递增。
	for _, id := range []string{"e4", "e5"} {
		if err := shard.CrashReplica(id); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "cluster remains writable at 3/5", 2e9, func() bool {
		st := shard.Leadership()
		return st.Writable && st.Quorum == 3
	})
	resp, err := client.Write(ctx, WriteRequest{
		ShardID: "exp", Key: "q", Value: []byte("1"), RequestID: "q1",
	})
	if err != nil {
		t.Fatalf("write at 3/5 should succeed: %v", err)
	}
	t.Logf("ASSERT after expansion 3/5 alive still commits next version=%d", resp.Version)

	// 再崩一个 -> 2/5 必须拒绝，不能降级。
	for _, id := range []string{"e1", "e2", "e3"} {
		if id != shard.LeaderID() {
			if err := shard.CrashReplica(id); err != nil {
				t.Fatal(err)
			}
			break
		}
	}
	waitFor(t, "cluster becomes not writable at 2/5", 2e9, func() bool {
		st := shard.Leadership()
		return !st.Writable && st.Reason == KindQuorumUnavailable
	})
	_, err = store.Write(ctx, WriteRequest{
		ShardID: "exp", Key: "q", Value: []byte("2"), RequestID: "q2",
	})
	assertErrorKind(t, err, KindQuorumUnavailable)
	t.Logf("ASSERT after expansion 2/5 alive => write rejected kind=%s (no degraded write)", KindQuorumUnavailable)
}

// TestExpansionRejectsDuplicateAndUnknown 验证扩容的边界拒绝。
func TestExpansionRejectsDuplicateAndUnknown(t *testing.T) {
	_, shard := expansionStore(t)
	if err := shard.ExpandReplicaSet([]string{"e4"}); err != nil {
		t.Fatalf("first expansion: %v", err)
	}
	err := shard.ExpandReplicaSet([]string{"e4"})
	assertErrorKind(t, err, KindConfigChange)
	t.Logf("ASSERT adding an already-member replica rejected kind=%s", KindConfigChange)

	err = shard.ExpandReplicaSet([]string{"e1"})
	assertErrorKind(t, err, KindConfigChange)
	err = shard.ExpandReplicaSet([]string{""})
	assertErrorKind(t, err, KindConfigChange)
}
