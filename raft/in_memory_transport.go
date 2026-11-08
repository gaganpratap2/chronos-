package raft

import (
	"context"
	"errors"
	"sync"
)

var (
	ErrInvalidPeerID   = errors.New("peer ID cannot be empty")
	ErrPeerUnavailable = errors.New("raft peer is unavailable")
)

type InMemoryVoteTransport struct {
	mu    sync.RWMutex
	nodes map[string]*Node
}

// NewInMemoryVoteTransport creates an empty transport.
func NewInMemoryVoteTransport() *InMemoryVoteTransport {
	return &InMemoryVoteTransport{
		nodes: make(map[string]*Node),
	}
}

// Register adds a node to the in-memory transport.
func (t *InMemoryVoteTransport) Register(peerID string, node *Node) error {
	if peerID == "" {
		return ErrInvalidPeerID
	}

	if node == nil {
		return ErrPeerUnavailable
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	t.nodes[peerID] = node
	return nil
}

func (t *InMemoryVoteTransport) RequestVote(ctx context.Context, peerID string, req RequestVoteRequest) (RequestVoteResponse, error) {

	if err := ctx.Err(); err != nil {
		return RequestVoteResponse{}, err
	}

	t.mu.RLock()
	node, exists := t.nodes[peerID]
	t.mu.RUnlock()

	if !exists {
		return RequestVoteResponse{}, ErrPeerUnavailable
	}

	return node.RequestVote(req)
}
