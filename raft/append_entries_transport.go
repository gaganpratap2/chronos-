package raft

import "context"

// AppendEntriesTransport sends AppendEntries RPCs to other Raft nodes.
type AppendEntriesTransport interface {
	AppendEntries(
		ctx context.Context,
		peerID string,
		req AppendEntriesRequest,
	) (AppendEntriesResponse, error)
}
