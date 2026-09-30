package store

import (
	"context"
	"log"
	"os"
	"testing"
	"time"
)

func newTestLogger(t *testing.T) Logger {
	return log.New(os.Stdout, "    ", log.Ltime|log.Lmicroseconds)
}

// newOneShardCluster 构造 1 个分片、replicas 个副本的测试集群。
// 键区间为整个字符串空间。dataDir 为空表示纯内存（崩溃模拟重启时会丢盘，
// 需要跨重启验证持久化的用例请使用 newOneShardClusterDir）。
func newOneShardCluster(t *testing.T, replicas int, dir string) (*Cluster, []string) {
	t.Helper()
	ids := make([]string, replicas)
	for i := range ids {
		ids[i] = nodeName(i)
	}
	router, err := NewRouter([]ShardConfig{{
		ID: 0, Range: Range{Start: "", End: ""}, Replicas: ids,
	}})
	if err != nil {
		t.Fatalf("router: %v", err)
	}
	net := NewNetwork()
	c := NewCluster(router, net, dir, newTestLogger(t))
	for _, id := range ids {
		if err := c.AddReplica(0, id); err != nil {
			t.Fatalf("add replica %s: %v", id, err)
		}
	}
	c.StartAll()
	return c, ids
}

func nodeName(i int) string {
	return "n" + itoa(i)
}

func TestSmokeBasicWriteRead(t *testing.T) {
	c, ids := newOneShardCluster(t, 3, "")
	defer c.ShutdownAll()

	leader, err := c.WaitForLeader(0, 2*time.Second)
	if err != nil {
		t.Fatalf("wait leader: %v", err)
	}
	t.Logf("leader elected: %s among %v", leader, ids)

	cl := NewClient(c)
	ctx := context.Background()
	wr, err := cl.Put(ctx, 0, "k1", []byte("v1"), -1)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	t.Logf("put k1=v1 version=%d leader=%s", wr.Version, wr.Leader)
	if wr.Version != 1 {
		t.Fatalf("want version 1, got %d", wr.Version)
	}

	rr, err := cl.Get(ctx, 0, "k1", ReadStrong)
	if err != nil {
		t.Fatalf("strong get: %v", err)
	}
	if string(rr.Item.Value) != "v1" || rr.Item.Version != 1 {
		t.Fatalf("strong get mismatch: %+v", rr)
	}
}
