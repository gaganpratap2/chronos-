package raft

import (
	"errors"
	"testing"
)

func TestStartElection(t *testing.T) {
	node, err := NewNode("node-A", []string{"node-B", "node-C"})
	if err != nil {
		t.Fatal(err)
	}

	if err := node.StartElection(); err != nil {
		t.Fatal(err)
	}

	if node.State() != StateCandidate {
		t.Errorf("State() = %v, want candidate", node.State())
	}

	if node.CurrentTerm() != 1 {
		t.Errorf("CurrentTerm() = %d, want 1", node.CurrentTerm())
	}

	if node.VotedFor() != "node-A" {
		t.Errorf("VotedFor() = %q, want node-A", node.VotedFor())
	}
}

func TestStartElectionIncrementsTerm(t *testing.T) {
	node, err := NewNode("node-A", nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := node.StartElection(); err != nil {
		t.Fatal(err)
	}

	if err := node.StartElection(); err != nil {
		t.Fatal(err)
	}

	if node.CurrentTerm() != 2 {
		t.Errorf("CurrentTerm() = %d, want 2", node.CurrentTerm())
	}
}

func TestBecomeLeader(t *testing.T) {
	node, err := NewNode("node-A", nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := node.StartElection(); err != nil {
		t.Fatal(err)
	}

	if err := node.BecomeLeader(); err != nil {
		t.Fatal(err)
	}

	if node.State() != StateLeader {
		t.Errorf("State() = %v, want leader", node.State())
	}
}

func TestBecomeLeaderRejectsFollower(t *testing.T) {
	node, err := NewNode("node-A", nil)
	if err != nil {
		t.Fatal(err)
	}

	err = node.BecomeLeader()

	if !errors.Is(err, ErrNotCandidate) {
		t.Errorf("error = %v, want ErrNotCandidate", err)
	}
}

func TestObserveHigherTerm(t *testing.T) {
	node, err := NewNode("node-A", nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := node.StartElection(); err != nil {
		t.Fatal(err)
	}

	if err := node.BecomeLeader(); err != nil {
		t.Fatal(err)
	}

	if err := node.ObserveHigherTerm(5); err != nil {
		t.Fatal(err)
	}

	if node.CurrentTerm() != 5 {
		t.Errorf("CurrentTerm() = %d, want 5", node.CurrentTerm())
	}

	if node.State() != StateFollower {
		t.Errorf("State() = %v, want follower", node.State())
	}

	if node.VotedFor() != "" {
		t.Errorf("VotedFor() = %q, want empty", node.VotedFor())
	}
}

func TestObserveHigherTermRejectsOldTerm(t *testing.T) {
	node, err := NewNode("node-A", nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := node.StartElection(); err != nil {
		t.Fatal(err)
	}

	err = node.ObserveHigherTerm(1)

	if !errors.Is(err, ErrTermNotHigher) {
		t.Errorf("error = %v, want ErrTermNotHigher", err)
	}
}
