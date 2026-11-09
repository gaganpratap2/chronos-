package raft

import (
	"errors"
	"fmt"
	"sync"
)

var (
	ErrInvalidNodeID = errors.New("raft node ID cannot be empty")
	ErrInvalidPeers  = errors.New("raft peers contain an empty or duplicate ID")
)

// Node represents one participant in a Raft cluster.
type Node struct {
	mu sync.RWMutex

	id           string
	state        State
	currentTerm  uint64
	votedFor     string
	peers        []string
	storage      Storage
	lastLogIndex uint64
	lastLogTerm  uint64
	log          []LogEntry
	commitIndex  uint64
	nextIndex    map[string]uint64
	matchIndex   map[string]uint64
}

// NewNode creates a new Raft node.
// NewNode creates a Raft node with in-memory storage.
func NewNode(id string, peers []string) (*Node, error) {
	return NewNodeWithStorage(id, peers, NewMemoryStorage())
}

func NewNodeWithStorage(id string, peers []string, storage Storage) (*Node, error) {

	if id == "" {
		return nil, ErrInvalidNodeID
	}

	if storage == nil {
		return nil, ErrInvalidStorage
	}

	seen := make(map[string]struct{}, len(peers))

	for _, peerID := range peers {
		if peerID == "" || peerID == id {
			return nil, ErrInvalidPeers
		}

		if _, exists := seen[peerID]; exists {
			return nil, ErrInvalidPeers
		}

		seen[peerID] = struct{}{}
	}

	hardState, err := storage.LoadHardState()
	if err != nil {
		return nil, err
	}

	if err := validateHardState(hardState); err != nil {
		return nil, err
	}

	if hardState.VotedFor != "" {
		validVoter := hardState.VotedFor == id

		for _, peerID := range peers {
			if peerID == hardState.VotedFor {
				validVoter = true
				break
			}
		}

		if !validVoter {
			return nil, ErrInvalidHardState
		}
	}

	peerCopy := append([]string(nil), peers...)

	return &Node{
		id:          id,
		state:       StateFollower,
		currentTerm: hardState.CurrentTerm,
		votedFor:    hardState.VotedFor,
		peers:       peerCopy,
		storage:     storage,
	}, nil
}

// ID returns the node's unique ID.
func (n *Node) ID() string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.id
}

// State returns the node's current state.
func (n *Node) State() State {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.state
}

// CurrentTerm returns the current election term.
func (n *Node) CurrentTerm() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.currentTerm
}

// VotedFor returns the candidate this node voted for.
func (n *Node) VotedFor() string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.votedFor
}

// Peers returns a copy of the cluster's peer IDs.
func (n *Node) Peers() []string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return append([]string(nil), n.peers...)
}

// ClusterSize returns the total number of nodes,
// including this node.
func (n *Node) ClusterSize() int {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return len(n.peers) + 1
}

// Majority returns the number of votes needed
// to obtain a majority.
func (n *Node) Majority() int {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return (len(n.peers)+1)/2 + 1
}

// String returns a readable description of the node.
func (n *Node) String() string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return fmt.Sprintf(
		"Node{id:%s state:%s term:%d votedFor:%s}",
		n.id,
		n.state,
		n.currentTerm,
		n.votedFor,
	)
}
