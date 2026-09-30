package quorum

import "context"

// ReadMode selects strong or eventual consistency.
type ReadMode int

const (
	ReadModeUnspecified ReadMode = iota
	ReadModeStrong
	ReadModeEventual
)

// ReplicaState is the coarse local role of one replica.
type ReplicaState int

const (
	ReplicaStateFollower ReplicaState = iota
	ReplicaStateCandidate
	ReplicaStateLeader
)

// Value is one committed key/value revision.
type Value struct {
	Data        []byte
	Version     uint64
	CommitIndex uint64
	CommitTerm  uint64
	RequestID   string
}

// WriteRequest is an idempotent, optionally compare-and-swap write.
type WriteRequest struct {
	ShardID        string
	Key            string
	Value          []byte
	CompareVersion *uint64
	RequestID      string
}

// WriteResponse reports the committed revision and dedupe result.
type WriteResponse struct {
	ShardID     string
	Key         string
	Version     uint64
	CommitIndex uint64
	CommitTerm  uint64
	LeaderID    string
	RequestID   string
	Duplicate   bool
}

// ReadRequest identifies a key, consistency mode and optional replica.
type ReadRequest struct {
	ShardID   string
	Key       string
	Mode      ReadMode
	ReplicaID string
	MinIndex  uint64
}

// ReadResponse includes the replica and log index that served the read.
type ReadResponse struct {
	ShardID     string
	Key         string
	Value       *Value
	ReplicaID   string
	CommitIndex uint64
	CommitTerm  uint64
	Stale       bool
}

// Entry is one replicated log entry.
type Entry struct {
	Index          uint64
	Term           uint64
	RequestID      string
	Key            string
	Value          []byte
	CompareVersion *uint64
	Admin          bool
}

// ReplicaStatus is a test and diagnostics snapshot.
type ReplicaStatus struct {
	ID          string
	ShardID     string
	State       ReplicaState
	Alive       bool
	CurrentTerm uint64
	CommitIndex uint64
	LogLength   uint64
	DataVersion uint64
}

// Logger accepts structured diagnostic lines from quorum decisions.
type Logger interface {
	Printf(format string, args ...any)
}

// Operation represents the local client-facing API.
type Operation interface {
	Write(ctx context.Context, req WriteRequest) (*WriteResponse, error)
	Read(ctx context.Context, req ReadRequest) (*ReadResponse, error)
}

func cloneBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}

func cloneEntry(in Entry) Entry {
	in.Value = cloneBytes(in.Value)
	if in.CompareVersion != nil {
		v := *in.CompareVersion
		in.CompareVersion = &v
	}
	return in
}

func cloneValue(in *Value) *Value {
	if in == nil {
		return nil
	}
	out := *in
	out.Data = cloneBytes(in.Data)
	return &out
}

func sameEntryCommand(a, b Entry) bool {
	if a.Term != b.Term || a.RequestID != b.RequestID || a.Key != b.Key ||
		a.Admin != b.Admin || string(a.Value) != string(b.Value) {
		return false
	}
	if a.CompareVersion == nil || b.CompareVersion == nil {
		return a.CompareVersion == nil && b.CompareVersion == nil
	}
	return *a.CompareVersion == *b.CompareVersion
}

func cloneUint64Ptr(in *uint64) *uint64 {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}
