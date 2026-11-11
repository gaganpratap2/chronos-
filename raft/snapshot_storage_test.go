package raft

import (
	"errors"
	"testing"
)

func TestMemoryStorageSaveAndLoadSnapshot(t *testing.T) {
	storage := NewMemoryStorage()

	expected := Snapshot{
		LastIncludedIndex: 10,
		LastIncludedTerm:  3,
		Data:              []byte("chronos-state"),
		Configuration: ConfigurationState{
			OldVoters: []string{"node-A"},
		},
	}

	if err := storage.SaveSnapshot(expected); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}

	got, err := storage.LoadSnapshot()
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}

	if got.LastIncludedIndex != expected.LastIncludedIndex {
		t.Fatalf("index = %d, want %d",
			got.LastIncludedIndex, expected.LastIncludedIndex)
	}

	if got.LastIncludedTerm != expected.LastIncludedTerm {
		t.Fatalf("term = %d, want %d",
			got.LastIncludedTerm, expected.LastIncludedTerm)
	}

	if string(got.Data) != string(expected.Data) {
		t.Fatalf("data = %q, want %q", got.Data, expected.Data)
	}
}

func TestMemoryStorageRejectsInvalidSnapshot(t *testing.T) {
	storage := NewMemoryStorage()

	err := storage.SaveSnapshot(Snapshot{
		LastIncludedIndex: 10,
		LastIncludedTerm:  0,
	})

	if !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("expected ErrInvalidSnapshot, got %v", err)
	}
}

func TestMemoryStorageSnapshotDoesNotShareData(t *testing.T) {
	storage := NewMemoryStorage()

	original := Snapshot{
		LastIncludedIndex: 5,
		LastIncludedTerm:  2,
		Data:              []byte("chronos"),
		Configuration: ConfigurationState{
			OldVoters: []string{"node-A"},
		},
	}

	if err := storage.SaveSnapshot(original); err != nil {
		t.Fatal(err)
	}

	original.Data[0] = 'X'

	got, err := storage.LoadSnapshot()
	if err != nil {
		t.Fatal(err)
	}

	if got.Data[0] != 'c' {
		t.Fatal("storage snapshot shares data with original")
	}

	got.Data[0] = 'Y'

	second, err := storage.LoadSnapshot()
	if err != nil {
		t.Fatal(err)
	}

	if second.Data[0] != 'c' {
		t.Fatal("loaded snapshot shares data with stored snapshot")
	}
}

func TestMemoryStorageLoadSnapshotInitiallyEmpty(t *testing.T) {
	storage := NewMemoryStorage()

	got, err := storage.LoadSnapshot()
	if err != nil {
		t.Fatal(err)
	}

	if got.LastIncludedIndex != 0 || got.LastIncludedTerm != 0 {
		t.Fatalf("expected empty snapshot, got %+v", got)
	}
}
