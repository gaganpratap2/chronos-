package raft

import (
	"errors"
	"testing"
)

func TestValidateSnapshotAcceptsEmptySnapshot(t *testing.T) {
	snapshot := Snapshot{}

	if err := validateSnapshot(snapshot); err != nil {
		t.Fatalf("expected empty snapshot to be valid, got %v", err)
	}
}

func TestValidateSnapshotAcceptsValidSnapshot(t *testing.T) {
	snapshot := Snapshot{
		LastIncludedIndex: 10,
		LastIncludedTerm:  3,
		Data:              []byte("chronos-state"),
	}

	if err := validateSnapshot(snapshot); err != nil {
		t.Fatalf("expected valid snapshot, got %v", err)
	}
}

func TestValidateSnapshotRejectsIndexWithoutTerm(t *testing.T) {
	snapshot := Snapshot{
		LastIncludedIndex: 10,
		LastIncludedTerm:  0,
	}

	err := validateSnapshot(snapshot)

	if !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("expected ErrInvalidSnapshot, got %v", err)
	}
}

func TestValidateSnapshotRejectsTermWithoutIndex(t *testing.T) {
	snapshot := Snapshot{
		LastIncludedIndex: 0,
		LastIncludedTerm:  2,
	}

	err := validateSnapshot(snapshot)

	if !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("expected ErrInvalidSnapshot, got %v", err)
	}
}

func TestCloneSnapshotCopiesData(t *testing.T) {
	original := Snapshot{
		LastIncludedIndex: 5,
		LastIncludedTerm:  2,
		Data:              []byte("chronos-state"),
	}

	cloned := cloneSnapshot(original)

	if cloned.LastIncludedIndex != original.LastIncludedIndex {
		t.Fatal("snapshot index was not copied")
	}

	if cloned.LastIncludedTerm != original.LastIncludedTerm {
		t.Fatal("snapshot term was not copied")
	}

	if string(cloned.Data) != string(original.Data) {
		t.Fatal("snapshot data was not copied")
	}

	// Mutating the clone must not change the original.
	cloned.Data[0] = 'X'

	if original.Data[0] == 'X' {
		t.Fatal("clone shares its data with the original snapshot")
	}
}
