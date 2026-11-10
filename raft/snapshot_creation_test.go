package raft

import (
	"errors"
	"reflect"
	"testing"
)

var errSnapshotPersistenceFailed = errors.New(
	"snapshot persistence failed",
)

type snapshotTestStateMachine struct {
	*testStateMachine

	data     []byte
	err      error
	restored []byte
	events   []string
}

func (s *snapshotTestStateMachine) Snapshot() ([]byte, error) {
	if s.err != nil {
		return nil, s.err
	}

	return append([]byte(nil), s.data...), nil
}

func (s *snapshotTestStateMachine) Restore(data []byte) error {
	if s.err != nil {
		return s.err
	}

	s.restored = append([]byte(nil), data...)
	s.events = append(s.events, "restore")

	return nil
}

func (s *snapshotTestStateMachine) Apply(
	index uint64,
	command []byte,
) error {
	s.events = append(s.events, "apply")

	return s.testStateMachine.Apply(index, command)
}

type failingSnapshotStorage struct {
	*MemoryStorage

	saveErr error
}

func (s *failingSnapshotStorage) SaveSnapshot(
	snapshot Snapshot,
) error {
	if s.saveErr != nil {
		return s.saveErr
	}

	return s.MemoryStorage.SaveSnapshot(snapshot)
}

func newSnapshotCreationTestNode(
	t *testing.T,
	storage Storage,
) (*Node, *snapshotTestStateMachine) {
	t.Helper()

	node, err := NewNodeWithStorage("node-A", nil, storage)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	stateMachine := &snapshotTestStateMachine{
		testStateMachine: &testStateMachine{},
		data:             []byte("state-after-task-2"),
	}

	node.mu.Lock()

	node.log = []LogEntry{
		{Term: 1, Command: []byte("task-1")},
		{Term: 1, Command: []byte("task-2")},
		{Term: 2, Command: []byte("task-3")},
		{Term: 2, Command: []byte("task-4")},
	}

	node.commitIndex = 2
	node.lastLogIndex = 4
	node.lastLogTerm = 2

	node.mu.Unlock()

	if err := node.SetStateMachine(stateMachine); err != nil {
		t.Fatalf("set state machine: %v", err)
	}

	return node, stateMachine
}

func TestCreateSnapshotCompactsAppliedEntriesAndRetainsSuffix(
	t *testing.T,
) {
	storage := NewMemoryStorage()

	node, _ := newSnapshotCreationTestNode(t, storage)

	if err := node.CreateSnapshot(); err != nil {
		t.Fatalf("create snapshot: %v", err)
	}

	node.mu.RLock()

	if node.snapshotIndex != 2 {
		t.Errorf(
			"expected snapshot index 2, got %d",
			node.snapshotIndex,
		)
	}

	if node.snapshotTerm != 1 {
		t.Errorf(
			"expected snapshot term 1, got %d",
			node.snapshotTerm,
		)
	}

	if len(node.log) != 2 {
		t.Errorf(
			"expected 2 retained log entries, got %d",
			len(node.log),
		)
	} else {
		if string(node.log[0].Command) != "task-3" {
			t.Errorf(
				"expected first retained command task-3, got %q",
				node.log[0].Command,
			)
		}

		if string(node.log[1].Command) != "task-4" {
			t.Errorf(
				"expected second retained command task-4, got %q",
				node.log[1].Command,
			)
		}
	}

	if node.lastLogIndex != 4 {
		t.Errorf(
			"expected last log index 4, got %d",
			node.lastLogIndex,
		)
	}

	if node.lastLogTerm != 2 {
		t.Errorf(
			"expected last log term 2, got %d",
			node.lastLogTerm,
		)
	}

	node.mu.RUnlock()

	saved, err := storage.LoadSnapshot()
	if err != nil {
		t.Fatalf("load saved snapshot: %v", err)
	}

	if saved.LastIncludedIndex != 2 {
		t.Errorf(
			"expected saved snapshot index 2, got %d",
			saved.LastIncludedIndex,
		)
	}

	if saved.LastIncludedTerm != 1 {
		t.Errorf(
			"expected saved snapshot term 1, got %d",
			saved.LastIncludedTerm,
		)
	}

	if string(saved.Data) != "state-after-task-2" {
		t.Errorf(
			"unexpected snapshot data: %q",
			saved.Data,
		)
	}
}

func TestCreateSnapshotDoesNotCompactWhenPersistenceFails(
	t *testing.T,
) {
	baseStorage := NewMemoryStorage()

	storage := &failingSnapshotStorage{
		MemoryStorage: baseStorage,
		saveErr:       errSnapshotPersistenceFailed,
	}

	node, _ := newSnapshotCreationTestNode(t, storage)

	if err := node.CreateSnapshot(); !errors.Is(
		err,
		errSnapshotPersistenceFailed,
	) {
		t.Fatalf(
			"expected persistence error, got %v",
			err,
		)
	}

	node.mu.RLock()
	defer node.mu.RUnlock()

	if node.snapshotIndex != 0 {
		t.Errorf(
			"expected snapshot index to remain 0, got %d",
			node.snapshotIndex,
		)
	}

	if len(node.log) != 4 {
		t.Errorf(
			"expected original 4 log entries, got %d",
			len(node.log),
		)
	}
}

func TestCreateSnapshotRejectsUnsupportedStateMachine(
	t *testing.T,
) {
	node, err := NewNodeWithStorage(
		"node-A",
		nil,
		NewMemoryStorage(),
	)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	node.mu.Lock()

	node.log = []LogEntry{
		{Term: 1, Command: []byte("task-1")},
		{Term: 1, Command: []byte("task-2")},
	}

	node.commitIndex = 2
	node.lastLogIndex = 2
	node.lastLogTerm = 1

	node.mu.Unlock()

	stateMachine := &testStateMachine{}

	if err := node.SetStateMachine(stateMachine); err != nil {
		t.Fatalf("set state machine: %v", err)
	}

	if err := node.CreateSnapshot(); !errors.Is(
		err,
		ErrSnapshotStateMachineUnsupported,
	) {
		t.Fatalf(
			"expected unsupported state machine error, got %v",
			err,
		)
	}
}

func TestCreateSnapshotRejectsIndexWithNoAppliedEntries(
	t *testing.T,
) {
	node, err := NewNodeWithStorage(
		"node-A",
		nil,
		NewMemoryStorage(),
	)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	stateMachine := &snapshotTestStateMachine{
		testStateMachine: &testStateMachine{},
		data:             []byte("empty-state"),
	}

	if err := node.SetStateMachine(stateMachine); err != nil {
		t.Fatalf("set state machine: %v", err)
	}

	if err := node.CreateSnapshot(); !errors.Is(
		err,
		ErrInvalidSnapshotIndex,
	) {
		t.Fatalf(
			"expected invalid snapshot index error, got %v",
			err,
		)
	}
}

func TestCreateSnapshotRejectsSnapshotExportFailure(
	t *testing.T,
) {
	storage := NewMemoryStorage()

	node, stateMachine := newSnapshotCreationTestNode(t, storage)

	stateMachine.err = errors.New("snapshot export failed")

	before := make([]LogEntry, len(node.log))
	copy(before, node.log)

	if err := node.CreateSnapshot(); err == nil {
		t.Fatal("expected snapshot export error")
	}

	node.mu.RLock()
	defer node.mu.RUnlock()

	if node.snapshotIndex != 0 {
		t.Errorf(
			"expected snapshot index to remain 0, got %d",
			node.snapshotIndex,
		)
	}

	if !reflect.DeepEqual(node.log, before) {
		t.Error("log changed after snapshot export failure")
	}
}
