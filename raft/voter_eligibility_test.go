package raft

import (
	"errors"
	"testing"
)

func newRemovedNodeTestNode(t *testing.T) (*Node, *MemoryStorage) {
	t.Helper()

	storage := NewMemoryStorage()

	// node-A has been removed from the active voter configuration.
	if err := storage.SaveConfiguration(ConfigurationState{
		OldVoters: []string{"node-B", "node-C", "node-D"},
	}); err != nil {
		t.Fatalf("save configuration: %v", err)
	}

	node, err := NewNodeWithStorage(
		"node-A",
		[]string{"node-B", "node-C", "node-D"},
		storage,
	)
	if err != nil {
		t.Fatalf("create removed node: %v", err)
	}

	return node, storage
}

func TestRemovedNodeCannotStartElection(t *testing.T) {
	node, storage := newRemovedNodeTestNode(t)

	err := node.StartElection()
	if !errors.Is(err, ErrNotVoter) {
		t.Fatalf("StartElection() error = %v, want ErrNotVoter", err)
	}

	if node.State() != StateFollower {
		t.Fatalf("state = %v, want follower", node.State())
	}

	if node.CurrentTerm() != 0 {
		t.Fatalf("term = %d, want 0", node.CurrentTerm())
	}

	saved, err := storage.LoadHardState()
	if err != nil {
		t.Fatalf("load hard state: %v", err)
	}

	if saved.CurrentTerm != 0 || saved.VotedFor != "" {
		t.Fatalf("hard state changed unexpectedly: %+v", saved)
	}
}

func TestRemovedNodeCannotBecomeLeader(t *testing.T) {
	tests := []struct {
		name    string
		promote func(*Node) error
	}{
		{
			name: "legacy majority",
			promote: func(node *Node) error {
				return node.becomeLeaderIfMajority(1, 3)
			},
		},
		{
			name: "configuration quorum",
			promote: func(node *Node) error {
				return node.becomeLeaderIfConfigurationQuorum(
					1,
					[]string{"node-B", "node-C", "node-D"},
				)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node, _ := newRemovedNodeTestNode(t)

			// Arrange a candidate state to test the promotion guard directly.
			node.state = StateCandidate
			node.currentTerm = 1

			err := tt.promote(node)
			if !errors.Is(err, ErrNotVoter) {
				t.Fatalf("promotion error = %v, want ErrNotVoter", err)
			}

			if node.State() != StateCandidate {
				t.Fatalf("state = %v, want candidate", node.State())
			}
		})
	}
}

func TestRequestVoteAcceptsCandidateInNewJointVoterSet(t *testing.T) {
	storage := NewMemoryStorage()

	// node-D belongs to NewVoters but not OldVoters.
	// It must still be eligible during joint consensus.
	if err := storage.SaveConfiguration(ConfigurationState{
		OldVoters: []string{"node-A", "node-B", "node-C"},
		NewVoters: []string{"node-B", "node-C", "node-D"},
	}); err != nil {
		t.Fatalf("save joint configuration: %v", err)
	}

	node, err := NewNodeWithStorage(
		"node-A",
		[]string{"node-B", "node-C", "node-D"},
		storage,
	)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	response, err := node.RequestVote(RequestVoteRequest{
		Term:        1,
		CandidateID: "node-D",
	})
	if err != nil {
		t.Fatalf("RequestVote() error = %v", err)
	}

	if !response.VoteGranted {
		t.Fatal("expected vote for candidate in NewVoters")
	}

	if node.VotedFor() != "node-D" {
		t.Fatalf("VotedFor() = %q, want node-D", node.VotedFor())
	}
}
