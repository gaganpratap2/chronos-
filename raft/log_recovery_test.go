package raft

import (
	"errors"
	"testing"
)

func TestNewNodeRestoresPersistedLog(t *testing.T) {
	storage := NewMemoryStorage()

	want := LogState{
		FirstIndex: 1,
		Entries: []LogEntry{
			{Term: 1, Command: []byte("first")},
			{Term: 2, Command: []byte("second")},
		},
	}

	if err := storage.SaveLog(want); err != nil {
		t.Fatal(err)
	}

	node, err := NewNodeWithStorage("A", []string{"B", "C"}, storage)
	if err != nil {
		t.Fatalf("NewNodeWithStorage() error = %v", err)
	}

	if len(node.log) != 2 {
		t.Fatalf("restored log length = %d, want 2", len(node.log))
	}

	if node.lastLogIndex != 2 {
		t.Fatalf("lastLogIndex = %d, want 2", node.lastLogIndex)
	}

	if node.lastLogTerm != 2 {
		t.Fatalf("lastLogTerm = %d, want 2", node.lastLogTerm)
	}

	if node.log[0].Term != 1 ||
		string(node.log[0].Command) != "first" {
		t.Fatalf("first restored entry = %+v", node.log[0])
	}
}

func TestNewNodeRejectsLogStartingAfterUnexpectedIndex(t *testing.T) {
	storage := NewMemoryStorage()

	state := LogState{
		FirstIndex: 2,
		Entries: []LogEntry{
			{Term: 1, Command: []byte("unexpected")},
		},
	}

	if err := storage.SaveLog(state); err != nil {
		t.Fatal(err)
	}

	_, err := NewNodeWithStorage("A", []string{"B", "C"}, storage)
	if !errors.Is(err, ErrInvalidLogState) {
		t.Fatalf("NewNodeWithStorage() error = %v, want ErrInvalidLogState", err)
	}
}

func TestNewNodeRestoresLogAfterSnapshot(t *testing.T) {
	storage := NewMemoryStorage()

	snapshot := Snapshot{
		LastIncludedIndex: 3,
		LastIncludedTerm:  2,
		Data:              []byte("snapshot-state"),
		Configuration: ConfigurationState{
			OldVoters: []string{"A", "B", "C"},
		},
	}

	if err := storage.SaveSnapshot(snapshot); err != nil {
		t.Fatal(err)
	}

	logState := LogState{
		FirstIndex: 4,
		Entries: []LogEntry{
			{Term: 2, Command: []byte("fourth")},
			{Term: 3, Command: []byte("fifth")},
		},
	}

	if err := storage.SaveLog(logState); err != nil {
		t.Fatal(err)
	}

	node, err := NewNodeWithStorage("A", []string{"B", "C"}, storage)
	if err != nil {
		t.Fatalf("NewNodeWithStorage() error = %v", err)
	}

	if node.snapshotIndex != 3 {
		t.Fatalf("snapshotIndex = %d, want 3", node.snapshotIndex)
	}

	if node.lastLogIndex != 5 {
		t.Fatalf("lastLogIndex = %d, want 5", node.lastLogIndex)
	}

	if node.lastLogTerm != 3 {
		t.Fatalf("lastLogTerm = %d, want 3", node.lastLogTerm)
	}

	if len(node.log) != 2 {
		t.Fatalf("restored log length = %d, want 2", len(node.log))
	}
}

func TestNewNodeRestoresOverlappingLogAfterSnapshot(t *testing.T) {
	storage := NewMemoryStorage()

	if err := storage.SaveSnapshot(Snapshot{
		LastIncludedIndex: 3,
		LastIncludedTerm:  2,
		Data:              []byte("snapshot-state"),
		Configuration: ConfigurationState{
			OldVoters: []string{"A", "B", "C"},
		},
	}); err != nil {
		t.Fatal(err)
	}

	if err := storage.SaveLog(LogState{
		FirstIndex: 1,
		Entries: []LogEntry{
			{Term: 1, Command: []byte("first")},
			{Term: 1, Command: []byte("second")},
			{Term: 2, Command: []byte("third")},
			{Term: 3, Command: []byte("fourth")},
			{Term: 3, Command: []byte("fifth")},
		},
	}); err != nil {
		t.Fatal(err)
	}

	node, err := NewNodeWithStorage("A", []string{"B", "C"}, storage)
	if err != nil {
		t.Fatalf("NewNodeWithStorage() error = %v", err)
	}

	if node.snapshotIndex != 3 {
		t.Fatalf("snapshotIndex = %d, want 3", node.snapshotIndex)
	}

	if len(node.log) != 2 {
		t.Fatalf("restored log length = %d, want 2", len(node.log))
	}

	if node.lastLogIndex != 5 {
		t.Fatalf("lastLogIndex = %d, want 5", node.lastLogIndex)
	}

	if node.lastLogTerm != 3 {
		t.Fatalf("lastLogTerm = %d, want 3", node.lastLogTerm)
	}

	if string(node.log[0].Command) != "fourth" {
		t.Fatalf("first retained command = %q, want fourth", node.log[0].Command)
	}
}

func TestNewNodeRejectsOverlappingLogWithMismatchedSnapshotTerm(t *testing.T) {
	storage := NewMemoryStorage()

	if err := storage.SaveSnapshot(Snapshot{
		LastIncludedIndex: 3,
		LastIncludedTerm:  2,
		Data:              []byte("snapshot-state"),
		Configuration: ConfigurationState{
			OldVoters: []string{"A", "B", "C"},
		},
	}); err != nil {
		t.Fatal(err)
	}

	if err := storage.SaveLog(LogState{
		FirstIndex: 1,
		Entries: []LogEntry{
			{Term: 1, Command: []byte("first")},
			{Term: 1, Command: []byte("second")},
			{Term: 9, Command: []byte("conflicting-third")},
			{Term: 3, Command: []byte("fourth")},
		},
	}); err != nil {
		t.Fatal(err)
	}

	_, err := NewNodeWithStorage("A", []string{"B", "C"}, storage)
	if !errors.Is(err, ErrInvalidLogState) {
		t.Fatalf(
			"NewNodeWithStorage() error = %v, want ErrInvalidLogState",
			err,
		)
	}
}
