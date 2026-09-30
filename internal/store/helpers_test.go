package store

import (
	"context"
	"log"
	"os"
	"testing"
	"time"
)

func testLogger(t *testing.T) Logger {
	t.Helper()
	return log.New(os.Stdout, "    ", log.Ltime|log.Lmicroseconds)
}

// clusterSpec 描述一个测试集群的分片布局。
type clusterSpec struct {
	id       int
	replicas []string
	rng      Range
}

func nodeID(shard, idx int) string {
	return nameOf(shard, idx)
}

func nameOf(shard, idx int) string {
	return "s" + itoa(shard) + "-n" + itoa(idx)
}

// buildCluster 按 spec 构造多分片集群。
func buildCluster(t *testing.T, specs []clusterSpec, dir string) *Cluster {
	t.Helper()
	cfgs := make([]ShardConfig, 0, len(specs))
	for _, sp := range specs {
		cfgs = append(cfgs, ShardConfig{ID: sp.id, Range: sp.rng, Replicas: sp.replicas})
	}
	router, err := NewRouter(cfgs)
	if err != nil {
		t.Fatalf("router: %v", err)
	}
	c := NewCluster(router, NewNetwork(), dir, testLogger(t))
	for _, sp := range specs {
		for _, id := range sp.replicas {
			if err := c.AddReplica(sp.id, id); err != nil {
				t.Fatalf("add replica %s: %v", id, err)
			}
		}
	}
	c.StartAll()
	return c
}

// oneShard3 构造单分片 3 副本集群，返回集群与节点 ID（n0,n1,n2）。
func oneShard3(t *testing.T, dir string) (*Cluster, []string) {
	t.Helper()
	ids := []string{nodeID(0, 0), nodeID(0, 1), nodeID(0, 2)}
	c := buildCluster(t, []clusterSpec{{id: 0, replicas: ids, rng: Range{"", ""}}}, dir)
	return c, ids
}

func mustLeader(t *testing.T, c *Cluster, shard int) string {
	t.Helper()
	l, err := c.WaitForLeader(shard, 2*time.Second)
	if err != nil {
		t.Fatalf("wait leader shard %d: %v", shard, err)
	}
	return l
}

func mustPut(t *testing.T, c *Cluster, shard int, key, val string, ver int64) WriteResponse {
	t.Helper()
	cl := NewClient(c)
	wr, err := cl.Put(context.Background(), shard, key, []byte(val), ver)
	if err != nil {
		t.Fatalf("put %s=%s: %v", key, val, err)
	}
	return wr
}

func mustGetStrong(t *testing.T, c *Cluster, shard int, key string) string {
	t.Helper()
	cl := NewClient(c)
	rr, err := cl.Get(context.Background(), shard, key, ReadStrong)
	if err != nil {
		t.Fatalf("strong get %s: %v", key, err)
	}
	return string(rr.Item.Value)
}

// assertCode 断言错误的可区分原因码符合预期（这是"失败原因可区分"不变量的核心断言）。
func assertCode(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error code %s, got nil", want)
	}
	got := ErrorCode(err)
	if got != want {
		t.Fatalf("error code mismatch:\n  want=%s\n  got =%s\n  err =%v", want, got, err)
	}
	t.Logf("OK rejected as %s: %v", want, err)
}
