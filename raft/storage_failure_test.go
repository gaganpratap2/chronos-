package raft

import (
	"errors"
	"testing"
)

var errStorageUnavailable = errors.New("storage unavailable")

// failingStorage simulates a storage system that cannot save state.
type failingStorage struct {
	state    HardState
	failSave bool
}

func (s *failingStorage) LoadHardState() (HardState, error) {
	return s.state, nil
}

func (s *failingStorage) SaveHardState(state HardState) error {
	if s.failSave {
		return errStorageUnavailable
	}

	s.state = state
	return nil
}

func TestStartElectionDoesNotChangeStateWhenStorageFails(t *testing.T) {
	storage := &failingStorage{
		state: HardState{
			CurrentTerm: 4,
		},
		failSave: true,
	}

	node, err := NewNodeWithStorage("node-A", nil, storage)
	if err != nil {
		t.Fatal(err)
	}

	err = node.StartElection()

	if !errors.Is(err, errStorageUnavailable) {
		t.Fatalf("StartElection() error = %v, want storage error", err)
	}

	if node.CurrentTerm() != 4 {
		t.Errorf("CurrentTerm() = %d, want 4", node.CurrentTerm())
	}

	if node.State() != StateFollower {
		t.Errorf("State() = %v, want follower", node.State())
	}

	if node.VotedFor() != "" {
		t.Errorf("VotedFor() = %q, want empty", node.VotedFor())
	}

	if storage.state.CurrentTerm != 4 {
		t.Errorf("stored term = %d, want 4", storage.state.CurrentTerm)
	}
}

func TestObserveHigherTermDoesNotChangeStateWhenStorageFails(t *testing.T) {
	storage := &failingStorage{
		state: HardState{
			CurrentTerm: 4,
			VotedFor:    "node-B",
		},
		failSave: true,
	}

	node, err := NewNodeWithStorage(
		"node-A",
		[]string{"node-B", "node-C"},
		storage,
	)

	if err != nil {
		t.Fatal(err)
	}

	err = node.ObserveHigherTerm(5)

	if !errors.Is(err, errStorageUnavailable) {
		t.Fatalf("ObserveHigherTerm() error = %v, want storage error", err)
	}

	if node.CurrentTerm() != 4 {
		t.Errorf("CurrentTerm() = %d, want 4", node.CurrentTerm())
	}

	if node.State() != StateFollower {
		t.Errorf("State() = %v, want follower", node.State())
	}

	if node.VotedFor() != "node-B" {
		t.Errorf("VotedFor() = %q, want node-B", node.VotedFor())
	}

	if storage.state.CurrentTerm != 4 {
		t.Errorf("stored term = %d, want 4", storage.state.CurrentTerm)
	}

	if storage.state.VotedFor != "node-B" {
		t.Errorf("stored vote = %q, want node-B", storage.state.VotedFor)
	}
}
