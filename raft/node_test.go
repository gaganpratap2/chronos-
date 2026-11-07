package raft

import (
	"errors"
	"reflect"
	"testing"
)

func TestNewNode(t *testing.T) {
	peers := []string{"node-B", "node-C"}

	node, err := NewNode("node-A", peers)
	if err != nil {
		t.Fatalf("NewNode() error = %v", err)
	}

	if node.ID() != "node-A" {
		t.Errorf("ID() = %q, want node-A", node.ID())
	}

	if node.State() != StateFollower {
		t.Errorf("State() = %v, want follower", node.State())
	}

	if node.CurrentTerm() != 0 {
		t.Errorf("CurrentTerm() = %d, want 0", node.CurrentTerm())
	}

	if node.VotedFor() != "" {
		t.Errorf("VotedFor() = %q, want empty", node.VotedFor())
	}

	if node.ClusterSize() != 3 {
		t.Errorf("ClusterSize() = %d, want 3", node.ClusterSize())
	}

	if node.Majority() != 2 {
		t.Errorf("Majority() = %d, want 2", node.Majority())
	}

	if !reflect.DeepEqual(node.Peers(), peers) {
		t.Errorf("Peers() = %v, want %v", node.Peers(), peers)
	}
}

func TestNewNodeRejectsInvalidIDs(t *testing.T) {
	_, err := NewNode("", nil)

	if !errors.Is(err, ErrInvalidNodeID) {
		t.Errorf("error = %v, want ErrInvalidNodeID", err)
	}
}

func TestNewNodeRejectsInvalidPeers(t *testing.T) {
	tests := []struct {
		name  string
		id    string
		peers []string
	}{
		{
			name:  "empty peer",
			id:    "node-A",
			peers: []string{"node-B", ""},
		},
		{
			name:  "self as peer",
			id:    "node-A",
			peers: []string{"node-A"},
		},
		{
			name:  "duplicate peer",
			id:    "node-A",
			peers: []string{"node-B", "node-B"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewNode(tt.id, tt.peers)

			if !errors.Is(err, ErrInvalidPeers) {
				t.Errorf("error = %v, want ErrInvalidPeers", err)
			}
		})
	}
}

func TestNodeReturnsIndependentPeerCopy(t *testing.T) {
	peers := []string{"node-B", "node-C"}

	node, err := NewNode("node-A", peers)
	if err != nil {
		t.Fatal(err)
	}

	peers[0] = "changed"

	if node.Peers()[0] != "node-B" {
		t.Fatal("node's peers changed when caller modified original slice")
	}

	returnedPeers := node.Peers()
	returnedPeers[0] = "also-changed"

	if node.Peers()[0] != "node-B" {
		t.Fatal("node's peers changed when caller modified returned slice")
	}
}
