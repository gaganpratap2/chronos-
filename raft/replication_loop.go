package raft

import (
	"context"
	"errors"
	"time"
)

var (
	ErrInvalidHeartbeatInterval = errors.New(
		"heartbeat interval must be greater than zero",
	)

	ErrInvalidRequestTimeout = errors.New(
		"replication request timeout must be greater than zero",
	)
)

// ReplicationLoop periodically replicates log entries and sends
// heartbeats to all followers.
type ReplicationLoop struct {
	Node           *Node
	Transport      AppendEntriesTransport
	Interval       time.Duration
	RequestTimeout time.Duration
}

// timeoutAppendEntriesTransport applies a separate timeout to each
// network request.
type timeoutAppendEntriesTransport struct {
	transport AppendEntriesTransport
	timeout   time.Duration
}

func (t *timeoutAppendEntriesTransport) AppendEntries(
	ctx context.Context,
	peerID string,
	req AppendEntriesRequest,
) (AppendEntriesResponse, error) {
	requestCtx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()

	return t.transport.AppendEntries(requestCtx, peerID, req)
}

// Run starts the periodic replication loop.
func (r *ReplicationLoop) Run(ctx context.Context) error {
	if r.Node == nil {
		return ErrInvalidStorage
	}

	if r.Transport == nil {
		return ErrNilAppendEntriesTransport
	}

	if r.Interval <= 0 {
		return ErrInvalidHeartbeatInterval
	}

	if r.RequestTimeout <= 0 {
		return ErrInvalidRequestTimeout
	}

	transport := &timeoutAppendEntriesTransport{
		transport: r.Transport,
		timeout:   r.RequestTimeout,
	}

	// Attempt replication immediately.
	r.replicate(ctx, transport)

	ticker := time.NewTicker(r.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-ticker.C:
			r.replicate(ctx, transport)
		}
	}
}

// replicate sends requests to all followers.
// Individual follower failures do not stop the loop.
func (r *ReplicationLoop) replicate(
	ctx context.Context,
	transport AppendEntriesTransport,
) {
	if ctx.Err() != nil {
		return
	}

	if r.Node.State() != StateLeader {
		return
	}

	_, _ = r.Node.ReplicateToAllPeers(ctx, transport)
}
