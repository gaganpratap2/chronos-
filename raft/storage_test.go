package raft

import (
	"errors"
	"path/filepath"
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

func TestNodeRestoresDiskHardStateAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft", "hard-state.log")

	// Start the first instance of the node.
	storage1, err := NewDiskStorage(path)
	if err != nil {
		t.Fatalf("create first storage: %v", err)
	}

	node1, err := NewNodeWithStorage(
		"node-A",
		[]string{"node-B", "node-C"},
		storage1,
	)
	if err != nil {
		t.Fatalf("create first node: %v", err)
	}

	if err := node1.StartElection(); err != nil {
		t.Fatalf("start election: %v", err)
	}

	if node1.CurrentTerm() != 1 {
		t.Fatalf("expected term 1, got %d", node1.CurrentTerm())
	}

	if err := storage1.Close(); err != nil {
		t.Fatalf("close first storage: %v", err)
	}

	// Simulate a process restart using the same journal file.
	storage2, err := NewDiskStorage(path)
	if err != nil {
		t.Fatalf("create second storage: %v", err)
	}
	defer storage2.Close()

	node2, err := NewNodeWithStorage(
		"node-A",
		[]string{"node-B", "node-C"},
		storage2,
	)
	if err != nil {
		t.Fatalf("restore second node: %v", err)
	}

	if node2.CurrentTerm() != 1 {
		t.Fatalf("expected restored term 1, got %d", node2.CurrentTerm())
	}

	if node2.VotedFor() != "node-A" {
		t.Fatalf("expected restored vote for node-A, got %q", node2.VotedFor())
	}

	if node2.State() != StateFollower {
		t.Fatalf("expected restarted node to be follower, got %s", node2.State())
	}
}
