package store

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

// TestMultiShardRoutingAndIndependentFailover
// 不变量：不同分片按区间独立路由、独立选主与容错；一个分片主切换不影响另一个分片。
func TestMultiShardRoutingAndIndependentFailover(t *testing.T) {
	specs := []clusterSpec{
		{id: 0, replicas: []string{"a0", "a1", "a2"}, rng: Range{"", "m"}},
		{id: 1, replicas: []string{"b0", "b1", "b2"}, rng: Range{"m", ""}},
	}
	c := buildCluster(t, specs, "")
	defer c.ShutdownAll()
	mustLeader(t, c, 0)
	mustLeader(t, c, 1)
	cl := NewClient(c)
	ctx := context.Background()

	// 各写各的区间。
	if _, err := cl.Put(ctx, 0, "apple", []byte("fruit"), -1); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Put(ctx, 1, "zebra", []byte("animal"), -1); err != nil {
		t.Fatal(err)
	}

	// 让分片 0 的主崩溃，分片 1 不受影响。
	l0 := c.ShardLeader(0)
	if err := c.CrashNode(l0); err != nil {
		t.Fatal(err)
	}
	// 分片 1 仍可强一致读写（其主未受影响）。
	if got := mustGetStrong(t, c, 1, "zebra"); got != "animal" {
		t.Fatalf("shard 1 affected by shard 0 failover: %q", got)
	}
	// 分片 0 完成切换后继续服务。
	if _, err := cl.Put(ctx, 0, "apricot", []byte("stone-fruit"), -1); err != nil {
		t.Fatalf("shard 0 write after its own failover: %v", err)
	}
	t.Logf("OK shards fail over independently: shard0 leader crashed (%s), shard1 stayed available", l0)
}

// TestConcurrentReadWriteConsistency
// 不变量：并发读写下，强一致读到的永远是某个已提交版本（单调可见），
// 已提交写入不会丢失或重复；版本号严格连续。
func TestConcurrentReadWriteConsistency(t *testing.T) {
	c, _ := oneShard3(t, "")
	defer c.ShutdownAll()
	mustLeader(t, c, 0)
	cl := NewClient(c)
	ctx := context.Background()

	const writers = 4
	const perWriter = 15
	var wg sync.WaitGroup
	var writeErrN int32
	var mu sync.Mutex

	// 并发写：每个 writer 用 CAS 从前一版本推进；失败（冲突）就重读版本重试，
	// 最终总成功数必须恰好等于写入的不同版本数。
	done := make(chan struct{})
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				for {
					select {
					case <-done:
						return
					default:
					}
					cur, err := cl.Get(ctx, 0, "counter", ReadStrong)
					var ver int64
					if err != nil {
						if ErrorCode(err) == CodeKeyNotFound {
							ver = 0
						} else {
							mu.Lock()
							writeErrN++
							mu.Unlock()
							return
						}
					} else {
						ver = cur.Item.Version
					}
					val := fmt.Sprintf("w%d-i%d", w, i)
					_, err = cl.Put(ctx, 0, "counter", []byte(val), ver)
					if err == nil {
						break
					}
					if ErrorCode(err) != CodeCasConflict {
						mu.Lock()
						writeErrN++
						mu.Unlock()
						return
					}
					// CasConflict：重读版本再试，不静默覆盖。
				}
			}
		}(w)
	}

	// 并发强一致读：读到的版本必须单调非降（不能读到回滚/未提交状态）。
	readerDone := make(chan struct{})
	go func() {
		var lastVer int64
		for i := 0; i < 200; i++ {
			select {
			case <-done:
			default:
			}
			rr, err := cl.Get(ctx, 0, "counter", ReadStrong)
			if err == nil {
				if rr.Item.Version < lastVer {
					t.Errorf("invariant violated: strong read version went backwards %d -> %d",
						lastVer, rr.Item.Version)
				}
				lastVer = rr.Item.Version
			}
		}
		close(readerDone)
	}()

	wg.Wait()
	close(done)
	<-readerDone

	if writeErrN != 0 {
		t.Fatalf("had %d unexpected write errors", writeErrN)
	}
	final, err := cl.Get(ctx, 0, "counter", ReadStrong)
	if err != nil {
		t.Fatal(err)
	}
	if final.Item.Version != int64(writers*perWriter) {
		t.Fatalf("invariant violated: expected exactly %d committed versions, got %d (lost or duplicate write?)",
			writers*perWriter, final.Item.Version)
	}
	t.Logf("OK %d concurrent writes produced exactly %d sequential versions (no loss/dup)",
		writers*perWriter, final.Item.Version)
}

// TestDeleteSemantics
// 不复存在：删除后强一致读返回 KeyNotFound（墓碑），版本仍递增；
// 之后重新创建从下一版本开始，CAS 不能作用于已删除键的旧版本。
func TestDeleteSemantics(t *testing.T) {
	c, _ := oneShard3(t, "")
	defer c.ShutdownAll()
	mustLeader(t, c, 0)
	cl := NewClient(c)
	ctx := context.Background()

	w, err := cl.Put(ctx, 0, "ephemeral", []byte("x"), -1)
	if err != nil {
		t.Fatal(err)
	}
	dw, err := cl.Delete(ctx, 0, "ephemeral", w.Version)
	if err != nil {
		t.Fatal(err)
	}
	if dw.Version != 2 {
		t.Fatalf("delete should advance version to 2, got %d", dw.Version)
	}
	_, err = cl.Get(ctx, 0, "ephemeral", ReadStrong)
	assertCode(t, err, CodeKeyNotFound)

	// 用已删除的旧版本 CAS 必须冲突。
	_, err = cl.Put(ctx, 0, "ephemeral", []byte("back"), 1)
	assertCode(t, err, CodeCasConflict)
	// 无条件重建成功，版本从 3 开始（墓碑不回滚版本）。
	rw, err := cl.Put(ctx, 0, "ephemeral", []byte("back"), -1)
	if err != nil {
		t.Fatal(err)
	}
	if rw.Version != 3 {
		t.Fatalf("recreate after tombstone should be version 3, got %d", rw.Version)
	}
	t.Log("OK delete creates tombstone, versions keep advancing, stale CAS rejected")
}
