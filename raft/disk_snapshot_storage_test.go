package raft

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestDiskStorageSaveAndLoadSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hard-state.log")

	storage, err := NewDiskStorage(path)
	if err != nil {
		t.Fatalf("create storage: %v", err)
	}
	defer storage.Close()

	expected := Snapshot{
		LastIncludedIndex: 5,
		LastIncludedTerm:  2,
		Data:              []byte("state-machine-data"),
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

	if got.LastIncludedIndex != expected.LastIncludedIndex ||
		got.LastIncludedTerm != expected.LastIncludedTerm ||
		string(got.Data) != string(expected.Data) {
		t.Fatalf("expected %+v, got %+v", expected, got)
	}
}

func TestDiskStorageSnapshotRecoversAfterReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hard-state.log")

	storage, err := NewDiskStorage(path)
	if err != nil {
		t.Fatalf("create storage: %v", err)
	}

	expected := Snapshot{
		LastIncludedIndex: 8,
		LastIncludedTerm:  3,
		Data:              []byte("recovered-state"),
		Configuration: ConfigurationState{
			OldVoters: []string{"node-A"},
		},
	}

	if err := storage.SaveSnapshot(expected); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}

	if err := storage.Close(); err != nil {
		t.Fatalf("close storage: %v", err)
	}

	reopened, err := NewDiskStorage(path)
	if err != nil {
		t.Fatalf("reopen storage: %v", err)
	}
	defer reopened.Close()

	got, err := reopened.LoadSnapshot()
	if err != nil {
		t.Fatalf("load snapshot after restart: %v", err)
	}

	if got.LastIncludedIndex != expected.LastIncludedIndex ||
		got.LastIncludedTerm != expected.LastIncludedTerm ||
		string(got.Data) != string(expected.Data) {
		t.Fatalf("expected %+v, got %+v", expected, got)
	}
}

func TestDiskStorageLoadsLatestSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hard-state.log")

	storage, err := NewDiskStorage(path)
	if err != nil {
		t.Fatalf("create storage: %v", err)
	}
	defer storage.Close()

	first := Snapshot{
		LastIncludedIndex: 5,
		LastIncludedTerm:  2,
		Data:              []byte("first"),
		Configuration: ConfigurationState{
			OldVoters: []string{"node-A"},
		},
	}

	second := Snapshot{
		LastIncludedIndex: 10,
		LastIncludedTerm:  4,
		Data:              []byte("second"),
		Configuration: ConfigurationState{
			OldVoters: []string{"node-A"},
		},
	}

	if err := storage.SaveSnapshot(first); err != nil {
		t.Fatalf("save first snapshot: %v", err)
	}

	if err := storage.SaveSnapshot(second); err != nil {
		t.Fatalf("save second snapshot: %v", err)
	}

	got, err := storage.LoadSnapshot()
	if err != nil {
		t.Fatalf("load latest snapshot: %v", err)
	}

	if got.LastIncludedIndex != second.LastIncludedIndex ||
		got.LastIncludedTerm != second.LastIncludedTerm ||
		string(got.Data) != string(second.Data) {
		t.Fatalf("expected latest snapshot %+v, got %+v", second, got)
	}
}

func TestDiskStorageTruncatesIncompleteFinalSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hard-state.log")

	storage, err := NewDiskStorage(path)
	if err != nil {
		t.Fatalf("create storage: %v", err)
	}

	expected := Snapshot{
		LastIncludedIndex: 5,
		LastIncludedTerm:  2,
		Data:              []byte("valid"),
		Configuration: ConfigurationState{
			OldVoters: []string{"node-A"},
		},
	}

	if err := storage.SaveSnapshot(expected); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}

	if err := storage.Close(); err != nil {
		t.Fatalf("close storage: %v", err)
	}

	snapshotPath := path + ".snapshots"

	file, err := os.OpenFile(
		snapshotPath,
		os.O_APPEND|os.O_WRONLY,
		0600,
	)
	if err != nil {
		t.Fatalf("open snapshot journal: %v", err)
	}

	_, writeErr := file.WriteString(
		`{"LastIncludedIndex":8,"LastIncludedTerm":3`,
	)
	closeErr := file.Close()

	if writeErr != nil {
		t.Fatalf("write incomplete snapshot: %v", writeErr)
	}
	if closeErr != nil {
		t.Fatalf("close snapshot journal: %v", closeErr)
	}

	reopened, err := NewDiskStorage(path)
	if err != nil {
		t.Fatalf("reopen storage: %v", err)
	}
	defer reopened.Close()

	got, err := reopened.LoadSnapshot()
	if err != nil {
		t.Fatalf("recover snapshot: %v", err)
	}

	if got.LastIncludedIndex != expected.LastIncludedIndex {
		t.Fatalf("expected index %d, got %d",
			expected.LastIncludedIndex, got.LastIncludedIndex)
	}

	next := Snapshot{
		LastIncludedIndex: 8,
		LastIncludedTerm:  3,
		Data:              []byte("next"),
		Configuration: ConfigurationState{
			OldVoters: []string{"node-A"},
		},
	}

	if err := reopened.SaveSnapshot(next); err != nil {
		t.Fatalf("save after recovery: %v", err)
	}

	got, err = reopened.LoadSnapshot()
	if err != nil {
		t.Fatalf("load after recovery: %v", err)
	}

	if got.LastIncludedIndex != next.LastIncludedIndex {
		t.Fatalf("expected index %d, got %d",
			next.LastIncludedIndex, got.LastIncludedIndex)
	}
}

func TestDiskStorageRejectsCorruptCompleteSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hard-state.log")

	if err := os.WriteFile(
		path+".snapshots",
		[]byte("not-valid-json\n"),
		0600,
	); err != nil {
		t.Fatalf("write corrupt snapshot: %v", err)
	}

	storage, err := NewDiskStorage(path)
	if err != nil {
		t.Fatalf("create storage: %v", err)
	}
	defer storage.Close()

	_, err = storage.LoadSnapshot()

	if !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("expected ErrInvalidSnapshot, got %v", err)
	}
}

func TestDiskStorageRejectsInvalidSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hard-state.log")

	storage, err := NewDiskStorage(path)
	if err != nil {
		t.Fatalf("create storage: %v", err)
	}
	defer storage.Close()

	invalid := Snapshot{
		LastIncludedIndex: 5,
		LastIncludedTerm:  0,
	}

	if err := storage.SaveSnapshot(invalid); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("expected ErrInvalidSnapshot, got %v", err)
	}
}

func TestDiskStorageLoadSnapshotInitiallyEmpty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hard-state.log")

	storage, err := NewDiskStorage(path)
	if err != nil {
		t.Fatalf("create storage: %v", err)
	}
	defer storage.Close()

	got, err := storage.LoadSnapshot()
	if err != nil {
		t.Fatalf("load empty snapshot: %v", err)
	}

	if got.LastIncludedIndex != 0 ||
		got.LastIncludedTerm != 0 ||
		len(got.Data) != 0 {
		t.Fatalf("expected empty snapshot, got %+v", got)
	}
}

func TestDiskStorageSnapshotOperationsAfterClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hard-state.log")

	storage, err := NewDiskStorage(path)
	if err != nil {
		t.Fatalf("create storage: %v", err)
	}

	if err := storage.Close(); err != nil {
		t.Fatalf("close storage: %v", err)
	}

	if _, err := storage.LoadSnapshot(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("expected os.ErrClosed from load, got %v", err)
	}

	snapshot := Snapshot{
		LastIncludedIndex: 1,
		LastIncludedTerm:  1,
		Data:              []byte("data"),
		Configuration: ConfigurationState{
			OldVoters: []string{"node-A"},
		},
	}

	if err := storage.SaveSnapshot(snapshot); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("expected os.ErrClosed from save, got %v", err)
	}
}

func TestDiskStorageSaveSnapshotRecoversIncompleteTailBeforeAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hard-state.log")

	storage, err := NewDiskStorage(path)
	if err != nil {
		t.Fatalf("create storage: %v", err)
	}

	first := Snapshot{
		LastIncludedIndex: 5,
		LastIncludedTerm:  2,
		Data:              []byte("first"),
		Configuration: ConfigurationState{
			OldVoters: []string{"node-A"},
		},
	}

	if err := storage.SaveSnapshot(first); err != nil {
		t.Fatalf("save first snapshot: %v", err)
	}

	if err := storage.Close(); err != nil {
		t.Fatalf("close storage: %v", err)
	}

	file, err := os.OpenFile(
		path+".snapshots",
		os.O_APPEND|os.O_WRONLY,
		0600,
	)
	if err != nil {
		t.Fatalf("open snapshot journal: %v", err)
	}

	_, writeErr := file.WriteString(
		`{"LastIncludedIndex":8,"LastIncludedTerm":3`,
	)
	closeErr := file.Close()

	if writeErr != nil {
		t.Fatalf("write incomplete snapshot: %v", writeErr)
	}
	if closeErr != nil {
		t.Fatalf("close snapshot journal: %v", closeErr)
	}

	reopened, err := NewDiskStorage(path)
	if err != nil {
		t.Fatalf("reopen storage: %v", err)
	}
	defer reopened.Close()

	// Intentionally do not call LoadSnapshot before saving.
	second := Snapshot{
		LastIncludedIndex: 8,
		LastIncludedTerm:  3,
		Data:              []byte("second"),
		Configuration: ConfigurationState{
			OldVoters: []string{"node-A"},
		},
	}

	if err := reopened.SaveSnapshot(second); err != nil {
		t.Fatalf("save snapshot after incomplete tail: %v", err)
	}

	got, err := reopened.LoadSnapshot()
	if err != nil {
		t.Fatalf("load snapshot after recovery: %v", err)
	}

	if got.LastIncludedIndex != second.LastIncludedIndex ||
		got.LastIncludedTerm != second.LastIncludedTerm ||
		string(got.Data) != string(second.Data) {
		t.Fatalf("expected %+v, got %+v", second, got)
	}
}
