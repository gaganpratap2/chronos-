package raft

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestDiskStorageSaveAndLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft", "hard-state.log")

	storage, err := NewDiskStorage(path)
	if err != nil {
		t.Fatalf("create storage: %v", err)
	}
	defer storage.Close()

	expected := HardState{
		CurrentTerm: 2,
		VotedFor:    "node-A",
	}

	if err := storage.SaveHardState(expected); err != nil {
		t.Fatalf("save hard state: %v", err)
	}

	got, err := storage.LoadHardState()
	if err != nil {
		t.Fatalf("load hard state: %v", err)
	}

	if got != expected {
		t.Fatalf("expected %+v, got %+v", expected, got)
	}
}

func TestDiskStorageRecoversAfterReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hard-state.log")

	storage, err := NewDiskStorage(path)
	if err != nil {
		t.Fatalf("create storage: %v", err)
	}

	first := HardState{CurrentTerm: 2, VotedFor: "node-A"}
	second := HardState{CurrentTerm: 3, VotedFor: "node-B"}

	if err := storage.SaveHardState(first); err != nil {
		t.Fatalf("save first state: %v", err)
	}

	if err := storage.SaveHardState(second); err != nil {
		t.Fatalf("save second state: %v", err)
	}

	if err := storage.Close(); err != nil {
		t.Fatalf("close storage: %v", err)
	}

	reopened, err := NewDiskStorage(path)
	if err != nil {
		t.Fatalf("reopen storage: %v", err)
	}
	defer reopened.Close()

	got, err := reopened.LoadHardState()
	if err != nil {
		t.Fatalf("load after restart: %v", err)
	}

	if got != second {
		t.Fatalf("expected latest state %+v, got %+v", second, got)
	}
}

func TestDiskStorageTruncatesIncompleteFinalRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hard-state.log")

	storage, err := NewDiskStorage(path)
	if err != nil {
		t.Fatalf("create storage: %v", err)
	}

	expected := HardState{CurrentTerm: 2, VotedFor: "node-A"}

	if err := storage.SaveHardState(expected); err != nil {
		t.Fatalf("save hard state: %v", err)
	}

	if err := storage.Close(); err != nil {
		t.Fatalf("close storage: %v", err)
	}

	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatalf("open journal: %v", err)
	}

	_, writeErr := file.WriteString(`{"CurrentTerm":3,"VotedFor":"node-B"`)
	closeErr := file.Close()

	if writeErr != nil {
		t.Fatalf("write incomplete record: %v", writeErr)
	}
	if closeErr != nil {
		t.Fatalf("close journal: %v", closeErr)
	}

	reopened, err := NewDiskStorage(path)
	if err != nil {
		t.Fatalf("reopen storage: %v", err)
	}
	defer reopened.Close()

	got, err := reopened.LoadHardState()
	if err != nil {
		t.Fatalf("recover hard state: %v", err)
	}

	if got != expected {
		t.Fatalf("expected %+v, got %+v", expected, got)
	}

	newState := HardState{CurrentTerm: 3, VotedFor: "node-C"}

	if err := reopened.SaveHardState(newState); err != nil {
		t.Fatalf("save after recovery: %v", err)
	}

	got, err = reopened.LoadHardState()
	if err != nil {
		t.Fatalf("load after new save: %v", err)
	}

	if got != newState {
		t.Fatalf("expected %+v after new save, got %+v", newState, got)
	}
}

func TestDiskStorageRejectsCorruptCompleteRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hard-state.log")

	if err := os.WriteFile(path, []byte("not-valid-json\n"), 0600); err != nil {
		t.Fatalf("write corrupt record: %v", err)
	}

	storage, err := NewDiskStorage(path)
	if err != nil {
		t.Fatalf("create storage: %v", err)
	}
	defer storage.Close()

	_, err = storage.LoadHardState()

	if !errors.Is(err, ErrInvalidHardState) {
		t.Fatalf("expected ErrInvalidHardState, got %v", err)
	}
}

func TestNewDiskStorageRejectsEmptyPath(t *testing.T) {
	_, err := NewDiskStorage("   ")

	if !errors.Is(err, ErrInvalidStoragePath) {
		t.Fatalf("expected ErrInvalidStoragePath, got %v", err)
	}
}

func TestDiskStorageCloseIsIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hard-state.log")

	storage, err := NewDiskStorage(path)
	if err != nil {
		t.Fatalf("create storage: %v", err)
	}

	if err := storage.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}

	if err := storage.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}
