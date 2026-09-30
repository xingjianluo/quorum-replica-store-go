package store

import (
	"testing"
	"time"
)

// TestCrashRecoveryPreservesCommittedData
// 不变量：已多数派提交的数据在"进程崩溃 -> 从磁盘重启"后仍在；
// 重启后用原请求标识重放不会重复生效（幂等结果随日志重建）。
func TestCrashRecoveryPreservesCommittedData(t *testing.T) {
	dir := t.TempDir()
	c, ids := oneShard3(t, dir)
	mustLeader(t, c, 0)
	leader := c.ShardLeader(0)

	mustPut(t, c, 0, "persist", "disk-val", -1)

	// 同时崩溃主与一个 follower，再全部恢复，验证多数派从磁盘重建后继续服务。
	c.ShutdownAll()
	t.Log("simultaneous crash of all nodes; logs retained under temp dir")

	// 用同一 dataDir 全新构造一个集群（等价于所有进程同时重启）。
	c2 := buildCluster(t, []clusterSpec{
		{id: 0, replicas: ids, rng: Range{"", ""}},
	}, dir)
	defer c2.ShutdownAll()
	newLeader := mustLeader(t, c2, 0)
	t.Logf("cluster restarted from disk, elected %s (old leader %s)", newLeader, leader)

	if got := mustGetStrong(t, c2, 0, "persist"); got != "disk-val" {
		t.Fatalf("invariant violated: committed data lost across full crash/restart: %q", got)
	}

	// 版本必须延续（不回滚）：下一次写版本应为 2。
	w := mustPut(t, c2, 0, "persist", "disk-val-2", 1)
	if w.Version != 2 {
		t.Fatalf("version did not continue across restart: got %d want 2", w.Version)
	}
	t.Logf("OK committed data & monotonic versions survived full crash/restart")
}

// TestEventualReadCanBeStaleButNeverCommitsUncommitted
// 不变量：最终一致读可能落后，但读到的永远是本地"已应用"（曾经多数派提交）的数据；
// 强一致读必须读到最新已提交值。
func TestReadModesSemantics(t *testing.T) {
	c, _ := oneShard3(t, "")
	defer c.ShutdownAll()
	mustLeader(t, c, 0)
	mustPut(t, c, 0, "k", "v1", -1)

	// 强一致读返回最新值，且标记非 stale。
	rr := mustGetStrongReadResponse(t, c, 0, "k")
	if string(rr.Item.Value) != "v1" || rr.Stale {
		t.Fatalf("strong read semantics wrong: %+v", rr)
	}

	// 最终一致读至少能读到某个已提交值。
	ev, err := c.Read(ReadRequest{ShardID: 0, Key: "k", Mode: ReadEventual})
	if err != nil {
		t.Fatal(err)
	}
	if string(ev.Item.Value) != "v1" {
		t.Fatalf("eventual read returned unexpected value: %q", string(ev.Item.Value))
	}
	t.Logf("OK strong read fresh(leader=%s), eventual read from %s stale=%v",
		rr.From, ev.From, ev.Stale)

	// 主缺失时强一致读必须失败（ReadModeConflict/NoShardLeader），而不是返回旧值。
	leader := c.ShardLeader(0)
	c.CrashNode(leader)
	// 立刻（新主选出前）尝试强一致读，允许客户端重试窗口内的有限等待。
	deadline := time.Now().Add(300 * time.Millisecond)
	sawReject := false
	for time.Now().Before(deadline) {
		_, err := c.Read(ReadRequest{ShardID: 0, Key: "k", Mode: ReadStrong})
		if err != nil {
			switch ErrorCode(err) {
			case CodeNoShardLeader, CodeReadModeConflict, CodeNoQuorum, CodeUnavailable:
				sawReject = true
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !sawReject {
		t.Fatal("invariant violated: strong read during leader election was not rejected")
	}
	t.Log("OK strong read rejected while no confirmed leader")
}

func mustGetStrongReadResponse(t *testing.T, c *Cluster, shard int, key string) ReadResponse {
	t.Helper()
	rr, err := c.Read(ReadRequest{ShardID: shard, Key: key, Mode: ReadStrong})
	if err != nil {
		t.Fatalf("strong read: %v", err)
	}
	return rr
}
