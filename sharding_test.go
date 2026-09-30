package quorum

import (
	"context"
	"testing"
)

func TestRangeShardBoundariesAndRouting(t *testing.T) {
	specs := []ShardSpec{
		{ID: "low", Range: ShardRange{Start: "", End: "m"}, ReplicaIDs: []string{"l1", "l2", "l3"}},
		{ID: "high", Range: ShardRange{Start: "m", End: ""}, ReplicaIDs: []string{"h1", "h2", "h3"}},
	}
	store, err := NewStore(specs)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	cases := []struct {
		key  string
		want string
	}{
		{"", ""},
		{"a", "low"},
		{"l", "low"},
		{"m", "high"},
		{"z", "high"},
		{"\x00", "low"},
		{"m\x00", "high"},
	}
	for _, tc := range cases {
		if tc.key == "" {
			_, err := store.ShardFor(tc.key)
			assertErrorKind(t, err, KindInvalidArgument)
			continue
		}
		set, err := store.ShardFor(tc.key)
		if err != nil {
			t.Fatalf("ShardFor(%q): %v", tc.key, err)
		}
		if set.Spec().ID != tc.want {
			t.Fatalf("key %q routed to %s, want %s", tc.key, set.Spec().ID, tc.want)
		}
		_, err = store.Write(ctx, WriteRequest{
			ShardID: "low", Key: tc.key, Value: []byte("x"), RequestID: "route-" + tc.key,
		})
		if tc.want == "low" && err != nil {
			t.Fatalf("low key %q rejected: %v", tc.key, err)
		}
		if tc.want == "high" {
			assertErrorKind(t, err, KindKeyOutOfRange)
		}
	}
}

func TestInvalidShardConfigurations(t *testing.T) {
	cases := []struct {
		name string
		spec []ShardSpec
	}{
		{
			name: "one replica",
			spec: []ShardSpec{{ID: "s", ReplicaIDs: []string{"only"}}},
		},
		{
			name: "first range does not start at minimum",
			spec: []ShardSpec{
				{ID: "a", Range: ShardRange{Start: "a", End: "m"}, ReplicaIDs: []string{"a1", "a2", "a3"}},
				{ID: "b", Range: ShardRange{Start: "m", End: ""}, ReplicaIDs: []string{"b1", "b2", "b3"}},
			},
		},
		{
			name: "last range does not end at maximum",
			spec: []ShardSpec{
				{ID: "a", Range: ShardRange{Start: "", End: "m"}, ReplicaIDs: []string{"a1", "a2", "a3"}},
				{ID: "b", Range: ShardRange{Start: "m", End: "z"}, ReplicaIDs: []string{"b1", "b2", "b3"}},
			},
		},
		{
			name: "range gap",
			spec: []ShardSpec{
				{ID: "a", Range: ShardRange{Start: "", End: "g"}, ReplicaIDs: []string{"a1", "a2", "a3"}},
				{ID: "b", Range: ShardRange{Start: "m", End: ""}, ReplicaIDs: []string{"b1", "b2", "b3"}},
			},
		},
		{
			name: "overlap",
			spec: []ShardSpec{
				{ID: "a", Range: ShardRange{Start: "", End: "n"}, ReplicaIDs: []string{"a1", "a2", "a3"}},
				{ID: "b", Range: ShardRange{Start: "m", End: ""}, ReplicaIDs: []string{"b1", "b2", "b3"}},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewStore(tc.spec)
			assertErrorKind(t, err, KindInvalidConfig)
		})
	}
}

func TestWritesAreIsolatedByOwningShard(t *testing.T) {
	ctx := context.Background()
	specs := []ShardSpec{
		{ID: "low", Range: ShardRange{Start: "", End: "m"}, ReplicaIDs: []string{"l1", "l2", "l3"}},
		{ID: "high", Range: ShardRange{Start: "m", End: ""}, ReplicaIDs: []string{"h1", "h2", "h3"}},
	}
	store, err := NewStore(specs)
	if err != nil {
		t.Fatal(err)
	}
	store.SetLogger(testLogger{t})

	lowResp, err := store.Write(ctx, WriteRequest{ShardID: "low", Key: "a", Value: []byte("low"), RequestID: "low-1"})
	if err != nil {
		t.Fatalf("low write: %v", err)
	}
	highResp, err := store.Write(ctx, WriteRequest{ShardID: "high", Key: "m", Value: []byte("high"), RequestID: "high-1"})
	if err != nil {
		t.Fatalf("high write: %v", err)
	}
	if lowResp.LeaderID == highResp.LeaderID {
		t.Fatalf("replica ids from different shards collided: %+v %+v", lowResp, highResp)
	}

	low, err := store.Read(ctx, ReadRequest{ShardID: "low", Key: "a", Mode: ReadModeStrong})
	if err != nil {
		t.Fatal(err)
	}
	high, err := store.Read(ctx, ReadRequest{ShardID: "high", Key: "m", Mode: ReadModeStrong})
	if err != nil {
		t.Fatal(err)
	}
	if string(low.Value.Data) != "low" || string(high.Value.Data) != "high" {
		t.Fatalf("cross-shard values leaked: low=%q high=%q", low.Value.Data, high.Value.Data)
	}
	if low.ReplicaID == high.ReplicaID {
		t.Fatalf("responses were served by replica ids from different shard namespaces: %+v %+v", low, high)
	}
	if low.ShardID != "low" || high.ShardID != "high" {
		t.Fatalf("unexpected response shards: %+v %+v", low, high)
	}
	if _, err := store.Shard("missing"); err == nil {
		t.Fatal("unknown shard lookup unexpectedly succeeded")
	} else {
		assertErrorKind(t, err, KindNoShard)
	}
}
