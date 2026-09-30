package store

import (
	"context"
	"sync/atomic"
)

// ReadMode 选择读语义。
type ReadMode int

const (
	// ReadStrong 强一致读：经主副本确认领导权后读已提交状态；主缺失/多数派不足时明确拒绝。
	ReadStrong ReadMode = iota
	// ReadEventual 最终一致读：从任一可达副本读本地已应用状态，可能落后但可用性更高。
	ReadEventual
)

// Client 是带自动重试的存储客户端。
// 每个写请求可由客户端生成唯一请求标识；重试时复用同一标识，保证不重复生效。
type Client struct {
	cluster *Cluster
	seq     atomic.Int64
}

// NewClient 构造客户端。
func NewClient(cluster *Cluster) *Client {
	// TODO(impl)
	return &Client{}
}

// NewRequestID 生成进程内唯一的请求标识。
func (c *Client) NewRequestID(prefix string) string {
	// TODO(impl)
	return ""
}

// Put 写入键值；expectedVersion < 0 表示无条件写。带自动重试与请求标识幂等。
func (c *Client) Put(ctx context.Context, shardID int, key string, value []byte, expectedVersion int64) (WriteResponse, error) {
	// TODO(impl)
	return WriteResponse{}, nil
}

// Delete 删除键；同样带版本检查与幂等。
func (c *Client) Delete(ctx context.Context, shardID int, key string, expectedVersion int64) (WriteResponse, error) {
	// TODO(impl)
	return WriteResponse{}, nil
}

// PutWithID 用显式请求标识写入（调用方可在多次重试间复用同一标识以验证幂等）。
func (c *Client) PutWithID(ctx context.Context, shardID int, key string, value []byte, expectedVersion int64, requestID string) (WriteResponse, error) {
	// TODO(impl)
	return WriteResponse{}, nil
}

// Get 按指定语义读键。
func (c *Client) Get(ctx context.Context, shardID int, key string, mode ReadMode) (ReadResponse, error) {
	// TODO(impl)
	return ReadResponse{}, nil
}
