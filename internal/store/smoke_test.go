package store

import (
	"context"
	"testing"
)

func TestSmokeBasicWriteRead(t *testing.T) {
	c, _ := oneShard3(t, "")
	defer c.ShutdownAll()
	leader := mustLeader(t, c, 0)
	t.Logf("leader elected: %s", leader)

	cl := NewClient(c)
	wr, err := cl.Put(context.Background(), 0, "k1", []byte("v1"), -1)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if wr.Version != 1 {
		t.Fatalf("want version 1, got %d", wr.Version)
	}
	rr, err := cl.Get(context.Background(), 0, "k1", ReadStrong)
	if err != nil {
		t.Fatalf("strong get: %v", err)
	}
	if string(rr.Item.Value) != "v1" || rr.Item.Version != 1 {
		t.Fatalf("strong get mismatch: %+v", rr)
	}
	// 多数派的 follower 也应当在短暂传播后持有该值（最终一致读）。
	ev, err := cl.Get(context.Background(), 0, "k1", ReadEventual)
	if err != nil || string(ev.Item.Value) != "v1" {
		t.Fatalf("eventual get mismatch: %v %+v", err, ev)
	}
}
