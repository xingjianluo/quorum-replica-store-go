package store

import (
	"context"
	"testing"
	"time"
)

// TestFailoverLeaderCrashElectsNewLeaderAndPreservesData
// 不变量：主崩溃后，剩余 2/3 副本仍形成多数派 -> 选出新主继续服务；
// 崩溃前已提交的数据在切换后可读到（不丢失、不回滚）。
func TestFailoverLeaderCrashElectsNewLeaderAndPreservesData(t *testing.T) {
	c, ids := oneShard3(t, "")
	defer c.ShutdownAll()
	old := mustLeader(t, c, 0)
	t.Logf("initial leader=%s", old)

	mustPut(t, c, 0, "k", "v1", -1)

	// 主崩溃。
	if err := c.CrashNode(old); err != nil {
		t.Fatal(err)
	}
	t.Logf("crashed old leader %s; waiting for re-election among remaining 2", old)

	// 客户端带重试的写应在新主上成功（验证自动切换对客户端透明）。
	cl := NewClient(c)
	wr, err := cl.Put(context.Background(), 0, "k", []byte("v2"), 1)
	if err != nil {
		t.Fatalf("write after failover: %v", err)
	}
	newLeader := wr.Leader
	if newLeader == old {
		t.Fatalf("new write must be served by a different leader, still %s", old)
	}
	t.Logf("new leader=%s served k=v2 version=%d", newLeader, wr.Version)

	// 旧的已提交数据仍在，且新写入生效。
	if got := mustGetStrong(t, c, 0, "k"); got != "v2" {
		t.Fatalf("invariant violated: committed data lost/rolled back after failover, got %q", got)
	}

	// 新主必须来自剩余副本集合。
	found := false
	for _, id := range ids {
		if id == newLeader {
			found = true
		}
	}
	if !found {
		t.Fatalf("new leader %s not a known replica %v", newLeader, ids)
	}
}

// TestFailoverMinorityCannotWrite
// 不变量：3 副本挂 2 后只剩 1 个，凑不齐多数派 -> 写入必须被拒绝（NoQuorum），
// 系统不得静默降级继续接受写。
func TestFailoverMinorityCannotWrite(t *testing.T) {
	c, ids := oneShard3(t, "")
	defer c.ShutdownAll()
	leader := mustLeader(t, c, 0)
	mustPut(t, c, 0, "k", "v1", -1)

	// 找到非主的两个副本并全部崩溃，制造只剩主（少数派）的局面。
	var downed []string
	for _, id := range ids {
		if id != leader {
			if err := c.CrashNode(id); err != nil {
				t.Fatal(err)
			}
			downed = append(downed, id)
		}
	}
	t.Logf("crashed majority replicas %v; leader %s isolated as minority", downed, leader)

	// 主在心跳失败后会因更高任期/无确认而下台；无论直接写主还是经客户端，都必须失败。
	// 先直接打主，预期 NotLeader/NoQuorum 之一；客户端重试到上限也必须返回 NoQuorum/NoShardLeader。
	deadline := time.Now().Add(2 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		n, _ := c.GetNode(leader)
		_, lastErr = n.Submit("minority-1", OpPut, "x", []byte("y"), -1)
		if lastErr != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if lastErr == nil {
		t.Fatal("invariant violated: write succeeded with only 1/3 replicas (no quorum)")
	}
	switch ErrorCode(lastErr) {
	case CodeNoQuorum, CodeNotLeader, CodeNoShardLeader:
		t.Logf("OK minority write rejected: %v", lastErr)
	default:
		t.Fatalf("invariant violated: minority write failed with non-quorum code: %v", lastErr)
	}

	// 强一致读也必须被拒绝（无法确认领导权），最终一致读在仅剩存活副本上仍允许。
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	cl := NewClient(c)
	if _, err := cl.Get(ctx, 0, "k", ReadStrong); err == nil {
		t.Fatal("invariant violated: strong read succeeded without quorum")
	} else {
		t.Logf("OK strong read rejected without quorum: %v", err)
	}
	if rr, err := cl.Get(context.Background(), 0, "k", ReadEventual); err != nil {
		t.Logf("eventual read on sole survivor unavailable: %v (acceptable)", err)
	} else {
		t.Logf("eventual read still served by %s (may be stale): %q", rr.From, string(rr.Item.Value))
	}

	// 恢复一个副本 -> 重新形成多数派，写入恢复。
	if err := c.RestartNode(downed[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Put(context.Background(), 0, "k", []byte("v3"), 1); err != nil {
		t.Fatalf("write after quorum restored: %v", err)
	}
	if got := mustGetStrong(t, c, 0, "k"); got != "v3" {
		t.Fatalf("data after quorum restore mismatch: %q", got)
	}
}

// TestFailoverOldLeaderRestartCannotOverwrite
// 不变量：旧主恢复后，不得用自己的旧日志覆盖新主已提交的数据。
func TestFailoverOldLeaderRestartCannotOverwrite(t *testing.T) {
	dir := t.TempDir() // 持久化目录，使崩溃/重启保留日志
	c, _ := oneShard3(t, dir)
	defer c.ShutdownAll()
	old := mustLeader(t, c, 0)
	mustPut(t, c, 0, "k", "old", -1)

	if err := c.CrashNode(old); err != nil {
		t.Fatal(err)
	}
	cl := NewClient(c)
	wr, err := cl.Put(context.Background(), 0, "k", []byte("new"), 1)
	if err != nil {
		t.Fatalf("write under new leader: %v", err)
	}
	newLeader := wr.Leader
	t.Logf("new leader %s committed k=new", newLeader)

	// 旧主带持久日志与持久任期恢复。
	if err := c.RestartNode(old); err != nil {
		t.Fatal(err)
	}
	t.Logf("old leader %s restarted with durable log; it must catch up, never overwrite", old)

	// 等待全部副本最终收敛到新值（旧主通过 AppendEntries 追赶，不得覆盖）。
	requireConverged := func() bool {
		for _, id := range mustReplicaIDs(t, c) {
			n, _ := c.GetNode(id)
			item, err := n.LocalRead("k")
			if err != nil || string(item.Value) != "new" {
				return false
			}
		}
		return true
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if requireConverged() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, id := range mustReplicaIDs(t, c) {
		n, _ := c.GetNode(id)
		item, err := n.LocalRead("k")
		if err != nil {
			t.Fatalf("replica %s missing k after catch-up: %v", id, err)
		}
		if string(item.Value) != "new" {
			t.Fatalf("invariant violated: old leader overwrote committed data on %s: got %q want %q",
				id, string(item.Value), "new")
		}
	}
	// 恢复后的旧主不得再次成为同任期主。
	if l := c.ShardLeader(0); l == old {
		st, _ := c.GetNode(old)
		t.Fatalf("restarted stale node %s reports itself leader (term=%d)", old, st.Term())
	}
}

func mustReplicaIDs(t *testing.T, c *Cluster) []string {
	t.Helper()
	ids, err := c.ShardReplicas(0)
	if err != nil {
		t.Fatal(err)
	}
	return ids
}
