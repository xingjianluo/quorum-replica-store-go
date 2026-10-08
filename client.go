package quorum

import (
	"context"
	"errors"
	"time"
)

// Client retries only idempotent, retryable failures.
type Client struct {
	store       *Store
	maxAttempts int
	retryDelay  time.Duration
	logger      Logger
}

// NewClient creates a retrying client. Zero maxAttempts defaults to 3.
func NewClient(store *Store, maxAttempts int, retryDelay time.Duration) *Client {
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	if retryDelay < 0 {
		retryDelay = 0
	}
	return &Client{
		store:       store,
		maxAttempts: maxAttempts,
		retryDelay:  retryDelay,
	}
}

// SetLogger records retry and final failure decisions.
func (c *Client) SetLogger(logger Logger) {
	c.logger = logger
}

func (c *Client) logf(format string, args ...any) {
	if c.logger != nil {
		c.logger.Printf(format, args...)
	}
}

// Write enforces a request ID and retries with that exact ID.
func (c *Client) Write(ctx context.Context, req WriteRequest) (*WriteResponse, error) {
	if c.store == nil {
		return nil, newError(KindInvalidConfig, "client-write", req.ShardID, "store is nil")
	}
	if req.RequestID == "" {
		return nil, newError(KindMissingRequestID, "client-write", req.ShardID,
			"client requires a request ID to safely retry")
	}
	var lastErr error
	for attempt := 1; attempt <= c.maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		resp, err := c.store.Write(ctx, req)
		if err == nil {
			if attempt > 1 {
				c.logf("write request=%s succeeded on attempt=%d duplicate=%t",
					req.RequestID, attempt, resp.Duplicate)
			}
			return resp, nil
		}
		lastErr = err
		var qerr *Error
		retryable := errors.As(err, &qerr) &&
			(qerr.Kind == KindQuorumUnavailable || qerr.Kind == KindNoLeader ||
				qerr.Kind == KindNotLeader || qerr.Kind == KindDurability)
		if !retryable || attempt == c.maxAttempts {
			c.logf("write request=%s final failure on attempt=%d retryable=%t: %v",
				req.RequestID, attempt, retryable, err)
			return nil, err
		}
		c.logf("write request=%s retry after attempt=%d delay=%s reason=%v",
			req.RequestID, attempt, c.retryDelay, err)
		if c.retryDelay > 0 {
			timer := time.NewTimer(c.retryDelay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
		}
	}
	return nil, lastErr
}

// Read retries strong reads after transient leader/quorum failures. Eventual
// reads are attempted once because a stale response is explicitly allowed.
func (c *Client) Read(ctx context.Context, req ReadRequest) (*ReadResponse, error) {
	if c.store == nil {
		return nil, newError(KindInvalidConfig, "client-read", req.ShardID, "store is nil")
	}
	if req.Mode == ReadModeEventual {
		return c.store.Read(ctx, req)
	}
	var lastErr error
	for attempt := 1; attempt <= c.maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		resp, err := c.store.Read(ctx, req)
		if err == nil {
			if attempt > 1 {
				c.logf("strong read key=%s succeeded on attempt=%d index=%d",
					req.Key, attempt, resp.CommitIndex)
			}
			return resp, nil
		}
		lastErr = err
		var qerr *Error
		retryable := errors.As(err, &qerr) &&
			(qerr.Kind == KindQuorumUnavailable || qerr.Kind == KindNoLeader ||
				qerr.Kind == KindNotLeader || qerr.Kind == KindStrongReadUnsafe)
		if !retryable || attempt == c.maxAttempts {
			c.logf("strong read key=%s final failure on attempt=%d retryable=%t: %v",
				req.Key, attempt, retryable, err)
			return nil, err
		}
		c.logf("strong read key=%s retry after attempt=%d delay=%s reason=%v",
			req.Key, attempt, c.retryDelay, err)
		if c.retryDelay > 0 {
			timer := time.NewTimer(c.retryDelay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
		}
	}
	return nil, lastErr
}
