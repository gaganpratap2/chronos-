package raft

import "context"

// VoteTransport sends RequestVote RPCs to other Raft nodes.
//
// The coordinator uses this interface instead of depending on
// a particular network protocol.
type VoteTransport interface {
	RequestVote(
		ctx context.Context,
		peerID string,
		req RequestVoteRequest,
	) (RequestVoteResponse, error)
}
