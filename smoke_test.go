package quorum

import (
	"context"
	"testing"
)

type testLogger struct{ t *testing.T }

func (l testLogger) Printf(format string, args ...any) {
	l.t.Helper()
	l.t.Logf(format, args...)
}

func testStore(t *testing.T) *Store {
	t.Helper()
	store, err := NewStore([]ShardSpec{{
		ID:         "users",
		Range:      ShardRange{},
		ReplicaIDs: []string{"r1", "r2", "r3"},
	}})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	store.SetLogger(testLogger{t})
	return store
}

func TestSmokeWriteStrongReadAndDuplicate(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	client := NewClient(store, 1, 0)
	client.SetLogger(testLogger{t})

	resp, err := client.Write(ctx, WriteRequest{
		ShardID:   "users",
		Key:       "alice",
		Value:     []byte("v1"),
		RequestID: "req-1",
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if resp.Version != 1 || resp.LeaderID == "" {
		t.Fatalf("unexpected response: %+v", resp)
	}

	again, err := client.Write(ctx, WriteRequest{
		ShardID:   "users",
		Key:       "alice",
		Value:     []byte("v1"),
		RequestID: "req-1",
	})
	if err != nil {
		t.Fatalf("duplicate Write: %v", err)
	}
	if !again.Duplicate || again.Version != 1 {
		t.Fatalf("duplicate was reapplied: %+v", again)
	}

	strong, err := client.Read(ctx, ReadRequest{ShardID: "users", Key: "alice", Mode: ReadModeStrong})
	if err != nil {
		t.Fatalf("strong Read: %v", err)
	}
	if string(strong.Value.Data) != "v1" || strong.Stale {
		t.Fatalf("unexpected strong read: %+v", strong)
	}

	eventual, err := store.Read(ctx, ReadRequest{
		ShardID:   "users",
		Key:       "alice",
		Mode:      ReadModeEventual,
		ReplicaID: "r3",
	})
	if err != nil {
		t.Fatalf("eventual Read: %v", err)
	}
	if string(eventual.Value.Data) != "v1" || eventual.Stale {
		t.Fatalf("unexpected eventual read: %+v", eventual)
	}
}
