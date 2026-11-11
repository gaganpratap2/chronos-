package raft

import (
	"errors"
	"reflect"
	"testing"
)

func newSnapshotRestoreTestNode(
	t *testing.T,
) (*Node, *MemoryStorage) {
	t.Helper()

	storage := NewMemoryStorage()

	snapshot := Snapshot{
		LastIncludedIndex: 2,
		LastIncludedTerm:  1,
		Data:              []byte("restored-state"),
		Configuration: ConfigurationState{
			OldVoters: []string{"node-A"},
		},
	}

	if err := storage.SaveSnapshot(snapshot); err != nil {
		t.Fatal(err)
	}

	node, err := NewNodeWithStorage("node-A", nil, storage)
	if err != nil {
		t.Fatal(err)
	}

	// Simulate retained entries after the persisted snapshot.
	node.mu.Lock()
	node.log = []LogEntry{
		{Term: 2, Command: []byte("task-3")},
		{Term: 2, Command: []byte("task-4")},
	}
	node.commitIndex = 4
	node.lastLogIndex = 4
	node.lastLogTerm = 2
	node.mu.Unlock()

	return node, storage
}

func TestSetStateMachineRestoresSnapshotBeforeApplyingSuffix(t *testing.T) {
	node, _ := newSnapshotRestoreTestNode(t)

	stateMachine := &snapshotTestStateMachine{
		testStateMachine: &testStateMachine{},
	}

	if err := node.SetStateMachine(stateMachine); err != nil {
		t.Fatalf("set state machine: %v", err)
	}

	if string(stateMachine.restored) != "restored-state" {
		t.Fatalf("restored data = %q", stateMachine.restored)
	}

	expectedIndexes := []uint64{3, 4}
	if !reflect.DeepEqual(stateMachine.applied, expectedIndexes) {
		t.Fatalf(
			"applied indexes = %v, want %v",
			stateMachine.applied,
			expectedIndexes,
		)
	}

	expectedEvents := []string{"restore", "apply", "apply"}
	if !reflect.DeepEqual(stateMachine.events, expectedEvents) {
		t.Fatalf(
			"events = %v, want %v",
			stateMachine.events,
			expectedEvents,
		)
	}

	if node.LastApplied() != 4 {
		t.Fatalf("last applied = %d, want 4", node.LastApplied())
	}
}

func TestSetStateMachineRejectsUnsupportedSnapshotRestorer(t *testing.T) {
	node, _ := newSnapshotRestoreTestNode(t)

	stateMachine := &testStateMachine{}

	err := node.SetStateMachine(stateMachine)
	if !errors.Is(err, ErrSnapshotRestoreUnsupported) {
		t.Fatalf(
			"expected ErrSnapshotRestoreUnsupported, got %v",
			err,
		)
	}

	node.mu.RLock()
	registered := node.stateMachine
	node.mu.RUnlock()

	if registered != nil {
		t.Fatal("unsupported state machine was registered")
	}
}

func TestSetStateMachineDoesNotRegisterAfterRestoreFailure(t *testing.T) {
	node, _ := newSnapshotRestoreTestNode(t)

	restoreErr := errors.New("snapshot restore failed")

	stateMachine := &snapshotTestStateMachine{
		testStateMachine: &testStateMachine{},
		err:              restoreErr,
	}

	err := node.SetStateMachine(stateMachine)
	if !errors.Is(err, restoreErr) {
		t.Fatalf("expected restore error, got %v", err)
	}

	node.mu.RLock()
	registered := node.stateMachine
	node.mu.RUnlock()

	if registered != nil {
		t.Fatal("state machine was registered after restore failure")
	}

	if node.LastApplied() != 2 {
		t.Fatalf("last applied = %d, want 2", node.LastApplied())
	}
}

func TestSetStateMachineRestoresIndependentSnapshotData(t *testing.T) {
	node, storage := newSnapshotRestoreTestNode(t)

	stateMachine := &snapshotTestStateMachine{
		testStateMachine: &testStateMachine{},
	}

	if err := node.SetStateMachine(stateMachine); err != nil {
		t.Fatal(err)
	}

	saved, err := storage.LoadSnapshot()
	if err != nil {
		t.Fatal(err)
	}

	if string(saved.Data) != "restored-state" {
		t.Fatalf(
			"stored snapshot was modified: %q",
			saved.Data,
		)
	}
}
