// Package store 实现一个本地可运行的分片复制键值存储：
// 按键区间分片，每分片多副本 Raft 风格多数派复制，支持主副本切换、
// 强一致/最终一致读、请求幂等与离线副本追赶。
package store

import (
	"errors"
	"fmt"
)

// 失败原因码：调用方可据此区分不同拒绝场景，禁止把这些错误合并成同一个原因。
const (
	CodeKeyOutOfRange       = "KeyOutOfRange"       // 键不属于任何已配置分片
	CodeNoShardLeader       = "NoShardLeader"       // 该分片当前没有已确认的主
	CodeNotLeader           = "NotLeader"           // 请求打到了非主副本
	CodeNoQuorum            = "NoQuorum"            // 存活/可达副本凑不出多数派
	CodeReplicasUnavailable = "ReplicasUnavailable" // 读要求的可达副本数不足
	CodeReadModeConflict    = "ReadModeConflict"    // 读语义与当前节点状态冲突（如旧主/未提交状态下做强一致读）
	CodeCasConflict         = "CasConflict"         // 版本冲突：expectedVersion 与当前版本不一致
	CodeKeyNotFound         = "KeyNotFound"         // 键不存在
	CodeShardUnavailable    = "ShardUnavailable"    // 分片因多数派丢失而不可写
	CodeUnavailable         = "Unavailable"         // 集群/节点当前不可用（崩溃、全隔离等）
)

// StoreError 携带可区分的失败原因码、发生位置与判定依据说明。
type StoreError struct {
	Code   string // 机器可判定的原因码
	Reason string // 人类可读的判定依据（例如投票/存活数明细）
	Cause  error  // 可选的底层错误
}

func (e *StoreError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("store error [%s]: %s: %v", e.Code, e.Reason, e.Cause)
	}
	return fmt.Sprintf("store error [%s]: %s", e.Code, e.Reason)
}

func (e *StoreError) Unwrap() error { return e.Cause }

// newError 构造一个带原因码的错误。
func newError(code, reason string) *StoreError {
	return &StoreError{Code: code, Reason: reason}
}

// wrapError 在原错误之上附加原因码。
func wrapError(code, reason string, cause error) *StoreError {
	return &StoreError{Code: code, Reason: reason, Cause: cause}
}

// ErrorCode 提取 StoreError 的原因码；非 StoreError 返回 CodeUnavailable。
func ErrorCode(err error) string {
	if err == nil {
		return ""
	}
	var se *StoreError
	if errors.As(err, &se) {
		return se.Code
	}
	return CodeUnavailable
}
