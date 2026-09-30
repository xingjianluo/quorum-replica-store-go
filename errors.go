package quorum

import (
	"errors"
	"fmt"
)

// ErrorKind 以稳定的机器可读分类区分拒绝原因。
type ErrorKind string

const (
	KindInvalidConfig      ErrorKind = "invalid_config"
	KindKeyOutOfRange      ErrorKind = "key_out_of_range"
	KindNoShard            ErrorKind = "no_shard"
	KindNoReplica          ErrorKind = "no_replica"
	KindQuorumUnavailable  ErrorKind = "quorum_unavailable"
	KindNoLeader           ErrorKind = "no_leader"
	KindNotLeader          ErrorKind = "not_leader"
	KindReplicaUnavailable ErrorKind = "replica_unavailable"
	KindVersionConflict    ErrorKind = "version_conflict"
	KindDuplicateConflict  ErrorKind = "duplicate_conflict"
	KindMissingRequestID   ErrorKind = "missing_request_id"
	KindInvalidReadMode    ErrorKind = "invalid_read_mode"
	KindStrongReadUnsafe   ErrorKind = "strong_read_unsafe"
	KindInvalidArgument    ErrorKind = "invalid_argument"
)

// Error 携带操作、分片和判定细节，便于测试日志直接定位拒绝依据。
type Error struct {
	Kind    ErrorKind
	Op      string
	ShardID string
	Detail  string
	Err     error
}

func (e *Error) Error() string {
	var prefix string
	if e.Op != "" {
		prefix = fmt.Sprintf("%s: %s", e.Op, e.Kind)
	} else {
		prefix = string(e.Kind)
	}
	if e.ShardID != "" {
		prefix += " shard=" + e.ShardID
	}
	if e.Detail != "" {
		prefix += ": " + e.Detail
	}
	if e.Err != nil {
		prefix += ": " + e.Err.Error()
	}
	return prefix
}
func (e *Error) Unwrap() error { return e.Err }

func newError(kind ErrorKind, op, shardID, detail string) *Error {
	return &Error{Kind: kind, Op: op, ShardID: shardID, Detail: detail}
}

var (
	ErrKeyOutOfRange     = errors.New("key is not owned by the addressed shard")
	ErrQuorumUnavailable = errors.New("a live majority is unavailable")
	ErrVersionConflict   = errors.New("compare version does not match current version")
	ErrDuplicateConflict = errors.New("request ID was already committed with different parameters")
	ErrNoLeader          = errors.New("no leader")
)
