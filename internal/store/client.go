package store

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

// ReadMode 选择读语义。
type ReadMode int

const (
	// ReadStrong 强一致读：经主副本确认领导权后读已提交状态；主缺失/多数派不足时明确拒绝。
	ReadStrong ReadMode = iota
	// ReadEventual 最终一致读：从任一可达副本读本地已应用状态，可能落后但可用性更高。
	ReadEventual
)

// retryableCodes 是客户端会自动重试的"瞬时"原因：
// 主切换、选举中、暂时凑不齐多数派（注意：若多数派永久丢失，重试到上限后仍会如实失败）。
var retryableCodes = map[string]bool{
	CodeNotLeader:       true,
	CodeNoShardLeader:   true,
	CodeNoQuorum:        true,
	CodeUnavailable:     true,
	CodeReadModeConflict: true,
}

// Client 是带自动重试的存储客户端。
// 每个写请求携带唯一请求标识；重试时复用同一标识，保证不重复生效。
type Client struct {
	cluster *Cluster
	seq     atomic.Int64
}

// NewClient 构造客户端。
func NewClient(cluster *Cluster) *Client {
	return &Client{cluster: cluster}
}

// NewRequestID 生成进程内唯一的请求标识。
func (c *Client) NewRequestID(prefix string) string {
	n := c.seq.Add(1)
	return fmt.Sprintf("%s-%d-%d", prefix, time.Now().UnixNano(), n)
}

// Put 写入键值；expectedVersion < 0 表示无条件写。整个重试链复用同一个请求标识。
func (c *Client) Put(ctx context.Context, shardID int, key string, value []byte, expectedVersion int64) (WriteResponse, error) {
	return c.PutWithID(ctx, shardID, key, value, expectedVersion, c.NewRequestID("put"))
}

// Delete 删除键；整个重试链复用同一个请求标识。
func (c *Client) Delete(ctx context.Context, shardID int, key string, expectedVersion int64) (WriteResponse, error) {
	id := c.NewRequestID("del")
	return c.submitWithRetry(ctx, WriteRequest{
		ShardID: shardID, Op: OpDel, Key: key,
		ExpectedVersion: expectedVersion, RequestID: id,
	})
}

// PutWithID 用显式请求标识写入：调用方在进程重启/手工重放时复用同一标识即可验证幂等。
func (c *Client) PutWithID(ctx context.Context, shardID int, key string, value []byte, expectedVersion int64, requestID string) (WriteResponse, error) {
	return c.submitWithRetry(ctx, WriteRequest{
		ShardID: shardID, Op: OpPut, Key: key, Value: value,
		ExpectedVersion: expectedVersion, RequestID: requestID,
	})
}

// submitWithRetry 提交写并在"主切换/选举中/暂时不可达"时复用同一请求标识重试。
// 注意：复用标识是关键——否则在"主已提交、响应丢失"的窗口重试会导致重复生效。
func (c *Client) submitWithRetry(ctx context.Context, req WriteRequest) (WriteResponse, error) {
	backoff := 10 * time.Millisecond
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return WriteResponse{}, wrapError(CodeUnavailable, "client context done", err)
		}
		r, err := c.cluster.Write(req)
		if err == nil {
			return r, nil
		}
		if !retryableCodes[ErrorCode(err)] {
			return WriteResponse{}, err // 路由错误/CAS 冲突等：重试无意义，原样抛出
		}
		if attempt >= 50 { // 约 1s 重试窗口，上限后如实把可区分原因交给调用方
			return WriteResponse{}, err
		}
		select {
		case <-ctx.Done():
			return WriteResponse{}, wrapError(CodeUnavailable, "client context done", ctx.Err())
		case <-time.After(backoff):
		}
		if backoff < 100*time.Millisecond {
			backoff *= 2
		}
	}
}

// Get 按指定语义读键。强一致读同样会在主切换窗口重试；最终一致读不做重试（始终有值即返回）。
func (c *Client) Get(ctx context.Context, shardID int, key string, mode ReadMode) (ReadResponse, error) {
	backoff := 10 * time.Millisecond
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return ReadResponse{}, wrapError(CodeUnavailable, "client context done", err)
		}
		r, err := c.cluster.Read(ReadRequest{ShardID: shardID, Key: key, Mode: mode})
		if err == nil {
			return r, nil
		}
		if mode == ReadEventual || !retryableCodes[ErrorCode(err)] {
			return ReadResponse{}, err
		}
		if attempt >= 50 {
			return ReadResponse{}, err
		}
		select {
		case <-ctx.Done():
			return ReadResponse{}, wrapError(CodeUnavailable, "client context done", ctx.Err())
		case <-time.After(backoff):
		}
		if backoff < 100*time.Millisecond {
			backoff *= 2
		}
	}
}
