package store

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestIdempotentDuplicateSubmitAppliesOnce
// 不变量：同一请求标识重复提交（即使走多次客户端调用）只生效一次，版本不重复递增。
func TestIdempotentDuplicateSubmitAppliesOnce(t *testing.T) {
	c, _ := oneShard3(t, "")
	defer c.ShutdownAll()
	mustLeader(t, c, 0)

	const rid = "dedup-123"
	w1, err := c.Write(WriteRequest{ShardID: 0, Op: OpPut, Key: "dup", Value: []byte("a"),
		ExpectedVersion: -1, RequestID: rid})
	if err != nil {
		t.Fatal(err)
	}
	// 用完全相同的请求标识再提交不同的值：必须返回首次结果，值与版本都不得改变。
	w2, err := c.Write(WriteRequest{ShardID: 0, Op: OpPut, Key: "dup", Value: []byte("DIFFERENT"),
		ExpectedVersion: -1, RequestID: rid})
	if err != nil {
		t.Fatalf("duplicate should return first outcome, got error: %v", err)
	}
	if w2.Version != w1.Version {
		t.Fatalf("invariant violated: duplicate advanced version %d -> %d", w1.Version, w2.Version)
	}
	if got := mustGetStrong(t, c, 0, "dup"); got != "a" {
		t.Fatalf("invariant violated: duplicate changed value to %q", got)
	}
	t.Logf("OK duplicate request %s applied once: version=%d unchanged value=%q", rid, w1.Version, "a")

	// 并发用同一请求标识同时打 N 次：恰好生效一次。
	const sameID = "dedup-concurrent"
	var wg sync.WaitGroup
	var versions sync.Map
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w, err := c.Write(WriteRequest{ShardID: 0, Op: OpPut, Key: "cc", Value: []byte("x"),
				ExpectedVersion: -1, RequestID: sameID})
			if err == nil {
				versions.Store(w.Version, true)
			}
		}()
	}
	wg.Wait()
	count := 0
	versions.Range(func(_, _ any) bool { count++; return true })
	if count != 1 {
		t.Fatalf("invariant violated: concurrent duplicate produced %d distinct versions, want 1", count)
	}
	if got := mustGetStrong(t, c, 0, "cc"); got != "x" {
		t.Fatalf("concurrent dedup value mismatch: %q", got)
	}
	t.Logf("OK 10 concurrent submits with same request id collapsed to 1 version")
}

// TestCASConflictDoesNotSilentlyOverwrite
// 不变量：expectedVersion 不匹配时必须以 CasConflict 明确拒绝，绝不静默覆盖；
// 版本单调递增，失败不占版本。
func TestCASConflictDoesNotSilentlyOverwrite(t *testing.T) {
	c, _ := oneShard3(t, "")
	defer c.ShutdownAll()
	mustLeader(t, c, 0)

	w1 := mustPut(t, c, 0, "k", "v1", -1) // version 1
	// 基于过期版本 5 做 CAS，必须失败。
	_, err := c.Write(WriteRequest{ShardID: 0, Op: OpPut, Key: "k", Value: []byte("stale"),
		ExpectedVersion: 5, RequestID: "cas-stale"})
	assertCode(t, err, CodeCasConflict)

	// 数据仍是 v1，版本仍是 1（失败不占版本号）。
	if got := mustGetStrong(t, c, 0, "k"); got != "v1" {
		t.Fatalf("invariant violated: failed CAS overwrote data: %q", got)
	}
	w3 := mustPut(t, c, 0, "k", "v2", w1.Version) // 正确版本 CAS 成功
	if w3.Version != 2 {
		t.Fatalf("want version 2 after successful CAS, got %d", w3.Version)
	}

	// 两个并发写都基于 version=1（乐观锁竞争）：恰好一个成功，另一个必须 CasConflict。
	var wg sync.WaitGroup
	var okN, conflictN int32
	var mu sync.Mutex
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cl := NewClient(c)
			_, err := cl.PutWithID(context.Background(), 0, "race",
				[]byte("v"), 0, "cas-race-"+itoa(i))
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				okN++
			} else if ErrorCode(err) == CodeCasConflict {
				conflictN++
			}
		}(i)
	}
	wg.Wait()
	// 两个都是对不存在键（version 0）的首次写：第一个成功，第二个看到 version=1 而冲突。
	if okN != 1 || conflictN != 1 {
		t.Fatalf("invariant violated: CAS race want 1 ok/1 conflict, got ok=%d conflict=%d", okN, conflictN)
	}
	t.Logf("OK CAS race: exactly 1 success and 1 CasConflict")
}

// TestNetworkPartitionHealsAndConverges
// 不变量：少数派侧（旧主）在分区期间无法提交写；多数派侧选出新主继续写；
// 分区愈合后旧主以旧任期的写被围栏拒绝，状态收敛到多数派已提交值。
func TestNetworkPartitionHealsAndConverges(t *testing.T) {
	c, ids := oneShard3(t, "")
	defer c.ShutdownAll()
	leader := mustLeader(t, c, 0)
	mustPut(t, c, 0, "k", "before", -1)

	others := []string{}
	for _, id := range ids {
		if id != leader {
			others = append(others, id)
		}
	}
	// 把主隔离到少数派：它与另两个节点全断（两者仍互通、构成多数派）。
	c.Network().Isolate(leader, ids)
	t.Logf("isolated leader %s from %v (minority side)", leader, others)

	// 旧主上的强一致写必须被拒（无法确认多数派领导权）。
	nOld, _ := c.GetNode(leader)
	deadline := time.Now().Add(2 * time.Second)
	var oldErr error
	for time.Now().Before(deadline) {
		_, oldErr = nOld.Submit("stale-leader-write", OpPut, "k", []byte("from-old"), -1)
		if oldErr != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if oldErr == nil {
		t.Fatal("invariant violated: isolated old leader accepted a write without quorum")
	}
	t.Logf("OK isolated old leader write rejected: %v", oldErr)

	// 多数派侧继续提供服务。
	cl := NewClient(c)
	wr, err := cl.Put(context.Background(), 0, "k", []byte("after"), 1)
	if err != nil {
		t.Fatalf("majority-side write during partition: %v", err)
	}
	t.Logf("majority elected new leader %s, wrote k=after v%d", wr.Leader, wr.Version)
	if wr.Leader == leader {
		t.Fatal("partition: new leader must not be the isolated node")
	}

	// 愈合分区。
	c.Network().HealAll()
	t.Log("partition healed; waiting for convergence")

	// 旧主必须回退到 follower 并收敛，不得保留 "from-old"。
	conv := time.Now().Add(4 * time.Second)
	for time.Now().Before(conv) {
		all := true
		for _, id := range ids {
			n, _ := c.GetNode(id)
			item, err := n.LocalRead("k")
			if err != nil || string(item.Value) != "after" {
				all = false
			}
		}
		if all {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, id := range ids {
		n, _ := c.GetNode(id)
		item, err := n.LocalRead("k")
		if err != nil || string(item.Value) != "after" {
			t.Fatalf("invariant violated after heal on %s: value=%q err=%v (old leader must not commit stale write)",
				id, string(item.Value), err)
		}
	}
	t.Log("OK all replicas converged to majority value 'after'; stale leader fenced")
}

// TestLongOfflineReplicaCatchesUp
// 不变量：副本长时间离线（多轮写入后才恢复），最终通过主的复制补齐缺失日志并收敛。
func TestLongOfflineReplicaCatchesUp(t *testing.T) {
	dir := t.TempDir()
	c, ids := oneShard3(t, dir)
	defer c.ShutdownAll()
	mustLeader(t, c, 0)
	mustPut(t, c, 0, "k", "v0", -1)

	// 让一个非主副本长期离线。
	leader := c.ShardLeader(0)
	var offline string
	for _, id := range ids {
		if id != leader {
			offline = id
			break
		}
	}
	if err := c.CrashNode(offline); err != nil {
		t.Fatal(err)
	}
	t.Logf("took replica %s offline; committing 20 more writes", offline)

	// 多数派仍在，继续 20 轮写入（版本推进到 21）。
	cl := NewClient(c)
	for i := 2; i <= 21; i++ {
		if _, err := cl.Put(context.Background(), 0, "k",
			[]byte("v"+itoa(i)), int64(i-1)); err != nil {
			t.Fatalf("write %d during offline: %v", i, err)
		}
	}

	// 恢复离线副本：它日志落后 20 条，应被主追平。
	if err := c.RestartNode(offline); err != nil {
		t.Fatal(err)
	}
	t.Logf("restarted lagging replica %s; waiting for it to catch up 20 entries", offline)

	deadline := time.Now().Add(5 * time.Second)
	var item KVItem
	for time.Now().Before(deadline) {
		n, _ := c.GetNode(offline)
		var err error
		item, err = n.LocalRead("k")
		if err == nil && item.Version == 21 && string(item.Value) == "v21" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if item.Version != 21 || string(item.Value) != "v21" {
		t.Fatalf("invariant violated: offline replica did not converge: version=%d value=%q",
			item.Version, string(item.Value))
	}
	t.Logf("OK offline replica caught up to version %d value=%q", item.Version, string(item.Value))
}
