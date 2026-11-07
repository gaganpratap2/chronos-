package raft

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestMemoryStorageRejectsInvalidHardState(t *testing.T) {
	storage := NewMemoryStorage()

	err := storage.SaveHardState(HardState{
		CurrentTerm: 0,
		VotedFor:    "node-A",
	})

	if !errors.Is(err, ErrInvalidHardState) {
		t.Fatalf("expected ErrInvalidHardState, got %v", err)
	}
}

func TestDiskStorageRejectsInvalidHardState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hard-state.log")

	storage, err := NewDiskStorage(path)
	if err != nil {
		t.Fatalf("create storage: %v", err)
	}
	defer storage.Close()

	err = storage.SaveHardState(HardState{
		CurrentTerm: 0,
		VotedFor:    "node-A",
	})

	if !errors.Is(err, ErrInvalidHardState) {
		t.Fatalf("expected ErrInvalidHardState, got %v", err)
	}
}

func TestNewNodeRejectsUnknownPersistedVote(t *testing.T) {
	storage := NewMemoryStorage()

	err := storage.SaveHardState(HardState{
		CurrentTerm: 3,
		VotedFor:    "node-X",
	})
	if err != nil {
		t.Fatalf("save hard state: %v", err)
	}

	_, err = NewNodeWithStorage(
		"node-A",
		[]string{"node-B", "node-C"},
		storage,
	)

	if !errors.Is(err, ErrInvalidHardState) {
		t.Fatalf("expected ErrInvalidHardState, got %v", err)
	}
}

func TestNewNodeAcceptsVoteForConfiguredPeer(t *testing.T) {
	storage := NewMemoryStorage()

	expected := HardState{
		CurrentTerm: 3,
		VotedFor:    "node-B",
	}

	if err := storage.SaveHardState(expected); err != nil {
		t.Fatalf("save hard state: %v", err)
	}

	node, err := NewNodeWithStorage(
		"node-A",
		[]string{"node-B", "node-C"},
		storage,
	)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	if node.VotedFor() != "node-B" {
		t.Fatalf("expected vote for node-B, got %q", node.VotedFor())
	}
}
