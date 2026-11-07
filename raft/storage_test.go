package raft

import (
	"errors"
	"testing"
)

func TestMemoryStorageSaveAndLoad(t *testing.T) {
	storage := NewMemoryStorage()

	want := HardState{
		CurrentTerm: 5,
		VotedFor:    "node-B",
	}

	if err := storage.SaveHardState(want); err != nil {
		t.Fatal(err)
	}

	got, err := storage.LoadHardState()
	if err != nil {
		t.Fatal(err)
	}

	if got != want {
		t.Fatalf("LoadHardState() = %+v, want %+v", got, want)
	}
}

func TestNewNodeRestoresHardState(t *testing.T) {
	storage := NewMemoryStorage()

	want := HardState{
		CurrentTerm: 7,
		VotedFor:    "node-C",
	}

	if err := storage.SaveHardState(want); err != nil {
		t.Fatal(err)
	}

	node, err := NewNodeWithStorage(
		"node-A",
		[]string{"node-B", "node-C"},
		storage,
	)
	if err != nil {
		t.Fatal(err)
	}

	if node.CurrentTerm() != 7 {
		t.Errorf("CurrentTerm() = %d, want 7", node.CurrentTerm())
	}

	if node.VotedFor() != "node-C" {
		t.Errorf("VotedFor() = %q, want node-C", node.VotedFor())
	}

	if node.State() != StateFollower {
		t.Errorf("State() = %v, want follower", node.State())
	}
}

func TestNewNodeRejectsNilStorage(t *testing.T) {
	_, err := NewNodeWithStorage("node-A", nil, nil)

	if !errors.Is(err, ErrInvalidStorage) {
		t.Errorf("error = %v, want ErrInvalidStorage", err)
	}
}
