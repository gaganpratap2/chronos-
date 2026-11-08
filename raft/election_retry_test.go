package raft

import (
	"context"
	"testing"
	"time"
)

func TestElectionCoordinatorRetriesAfterSplitVote(t *testing.T) {
	nodeA, err := NewNode(
		"node-A",
		[]string{"node-B", "node-C", "node-D", "node-E"},
	)
	if err != nil {
		t.Fatal(err)
	}

	transport := NewInMemoryVoteTransport()

	coordinator, err := NewElectionCoordinator(nodeA, transport)
	if err != nil {
		t.Fatal(err)
	}

	config := ElectionConfig{
		MinTimeout: time.Millisecond,
		MaxTimeout: 2 * time.Millisecond,
	}

	elected, err := coordinator.RunElectionWithRetry(
		context.Background(),
		config,
		3,
	)
	if err != nil {
		t.Fatal(err)
	}

	if elected {
		t.Fatal("election should fail without a majority")
	}

	if nodeA.CurrentTerm() != 3 {
		t.Errorf(
			"term = %d, want 3 after three attempts",
			nodeA.CurrentTerm(),
		)
	}

	if nodeA.State() != StateCandidate {
		t.Errorf(
			"state = %v, want candidate",
			nodeA.State(),
		)
	}
}

func TestElectionCoordinatorRejectsInvalidRetryCount(t *testing.T) {
	node, err := NewNode("node-A", nil)
	if err != nil {
		t.Fatal(err)
	}

	coordinator, err := NewElectionCoordinator(
		node,
		NewInMemoryVoteTransport(),
	)
	if err != nil {
		t.Fatal(err)
	}

	config := ElectionConfig{
		MinTimeout: time.Millisecond,
		MaxTimeout: 2 * time.Millisecond,
	}

	_, err = coordinator.RunElectionWithRetry(
		context.Background(),
		config,
		0,
	)

	if err != ErrInvalidElectionAttempts {
		t.Fatalf(
			"error = %v, want %v",
			err,
			ErrInvalidElectionAttempts,
		)
	}
}
