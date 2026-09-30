package store

import (
	"testing"
)

func TestMajority(t *testing.T) {
	cases := map[int]int{1: 1, 2: 2, 3: 2, 5: 3, 7: 4}
	for n, want := range cases {
		if got := Majority(n); got != want {
			t.Fatalf("Majority(%d)=%d want %d", n, got, want)
		}
	}
}

func TestRouterContiguousRanges(t *testing.T) {
	r, err := NewRouter([]ShardConfig{
		{ID: 0, Range: Range{"", "m"}, Replicas: []string{"a", "b", "c"}},
		{ID: 1, Range: Range{"m", ""}, Replicas: []string{"d", "e", "f"}},
	})
	if err != nil {
		t.Fatalf("router: %v", err)
	}
	sc, err := r.Route("apple")
	if err != nil || sc.ID != 0 {
		t.Fatalf("apple -> shard %d err=%v", sc.ID, err)
	}
	sc, err = r.Route("mango")
	if err != nil || sc.ID != 1 {
		t.Fatalf("mango -> shard %d err=%v", sc.ID, err)
	}
	// 边界：'m' 属于右开区间起点，归分片 1
	sc, err = r.Route("m")
	if err != nil || sc.ID != 1 {
		t.Fatalf("'m' -> shard %d err=%v", sc.ID, err)
	}
}

func TestRouterRejectsNonContiguousAndEvenReplicas(t *testing.T) {
	if _, err := NewRouter([]ShardConfig{
		{ID: 0, Range: Range{"", "b"}, Replicas: []string{"a", "b", "c"}},
		{ID: 1, Range: Range{"c", ""}, Replicas: []string{"d", "e", "f"}}, // 空洞 b..c
	}); err == nil {
		t.Fatal("expected non-contiguous rejection")
	}
	if _, err := NewRouter([]ShardConfig{
		{ID: 0, Range: Range{"", ""}, Replicas: []string{"a", "b"}}, // 偶数副本
	}); ErrorCode(err) != CodeReplicasUnavailable {
		t.Fatalf("want %s, got %v", CodeReplicasUnavailable, err)
	}
}

func TestWriteKeyNotInShardRejected(t *testing.T) {
	c := buildCluster(t, []clusterSpec{
		{id: 0, replicas: []string{"a", "b", "c"}, rng: Range{"", "m"}},
		{id: 1, replicas: []string{"d", "e", "f"}, rng: Range{"m", ""}},
	}, "")
	defer c.ShutdownAll()
	mustLeader(t, c, 0)
	mustLeader(t, c, 1)
	// 键 "z" 属于分片 1，显式写分片 0 必须以 KeyOutOfRange 拒绝。
	_, err := c.Write(WriteRequest{ShardID: 0, Key: "z", Op: OpPut, Value: []byte("v"),
		ExpectedVersion: -1, RequestID: "r1"})
	assertCode(t, err, CodeKeyOutOfRange)
}

func TestReadMissingKeyDistinctFromUnavailable(t *testing.T) {
	c, _ := oneShard3(t, "")
	defer c.ShutdownAll()
	mustLeader(t, c, 0)
	_, err := c.Read(ReadRequest{ShardID: 0, Key: "absent", Mode: ReadStrong})
	// 键不存在是正常业务结果（KeyNotFound），必须能和分片不可用区分开。
	assertCode(t, err, CodeKeyNotFound)
}
