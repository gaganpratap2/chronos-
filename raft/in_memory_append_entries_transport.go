package raft

import (
	"context"
	"sync"
)

// InMemoryAppendEntriesTransport delivers AppendEntries RPCs
// directly to registered Raft nodes.
type InMemoryAppendEntriesTransport struct {
	mu    sync.RWMutex
	nodes map[string]*Node
}

// NewInMemoryAppendEntriesTransport creates an empty transport.
func NewInMemoryAppendEntriesTransport() *InMemoryAppendEntriesTransport {
	return &InMemoryAppendEntriesTransport{
		nodes: make(map[string]*Node),
	}
}

// Register adds a node to the in-memory transport.
func (t *InMemoryAppendEntriesTransport) Register(
	peerID string,
	node *Node,
) error {
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

// AppendEntries sends an RPC to the requested follower.
func (t *InMemoryAppendEntriesTransport) AppendEntries(
	ctx context.Context,
	peerID string,
	req AppendEntriesRequest,
) (AppendEntriesResponse, error) {
	if err := ctx.Err(); err != nil {
		return AppendEntriesResponse{}, err
	}

	t.mu.RLock()
	node, exists := t.nodes[peerID]
	t.mu.RUnlock()

	if !exists {
		return AppendEntriesResponse{}, ErrPeerUnavailable
	}

	return node.AppendEntries(req)
}
