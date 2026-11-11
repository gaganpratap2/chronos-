package raft

import (
	"errors"
	"reflect"
	"testing"
)

var (
	errInstallSnapshotPersistence = errors.New(
		"install snapshot persistence failed",
	)
	errInstallSnapshotRestore = errors.New(
		"install snapshot restore failed",
	)
	errInstallHardStatePersistence = errors.New(
		"install hard state persistence failed",
	)
)

type snapshotInstallTestStorage struct {
	*MemoryStorage

	snapshotErr       error
	hardStateErr      error
	logSaveCalls      int
	failLogSaveAt     int
	snapshotSaveCalls int
}

func (s *snapshotInstallTestStorage) SaveSnapshot(
	snapshot Snapshot,
) error {
	s.snapshotSaveCalls++

	if s.snapshotErr != nil {
		return s.snapshotErr
	}

	return s.MemoryStorage.SaveSnapshot(snapshot)
}

func (s *snapshotInstallTestStorage) SaveLog(
	state LogState,
) error {
	s.logSaveCalls++

	if s.failLogSaveAt != 0 &&
		s.logSaveCalls == s.failLogSaveAt {
		return errStorageUnavailable
	}

	return s.MemoryStorage.SaveLog(state)
}

func (s *snapshotInstallTestStorage) SaveHardState(
	state HardState,
) error {
	if s.hardStateErr != nil {
		return s.hardStateErr
	}

	return s.MemoryStorage.SaveHardState(state)
}

func newSnapshotInstallTestNode(
	t *testing.T,
	storage Storage,
) (*Node, *snapshotTestStateMachine) {
	t.Helper()

	node, err := NewNodeWithStorage("follower-A", nil, storage)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	stateMachine := &snapshotTestStateMachine{
		testStateMachine: &testStateMachine{},
		data:             []byte("local-state"),
	}

	node.mu.Lock()

	node.log = []LogEntry{
		{Term: 1, Command: []byte("entry-1")},
		{Term: 1, Command: []byte("entry-2")},
		{Term: 2, Command: []byte("entry-3")},
		{Term: 2, Command: []byte("entry-4")},
		{Term: 3, Command: []byte("entry-5")},
	}

	node.lastLogIndex = 5
	node.lastLogTerm = 3
	node.commitIndex = 1

	node.mu.Unlock()

	if err := node.SetStateMachine(stateMachine); err != nil {
		t.Fatalf("set state machine: %v", err)
	}

	return node, stateMachine
}

func TestInstallSnapshotRestoresAndRetainsMatchingSuffix(
	t *testing.T,
) {
	storage := NewMemoryStorage()
	node, stateMachine := newSnapshotInstallTestNode(t, storage)

	req := InstallSnapshotRequest{
		Term:              4,
		LeaderID:          "leader-A",
		LastIncludedIndex: 3,
		LastIncludedTerm:  2,
		Data:              []byte("restored-state"),
		Configuration: ConfigurationState{
			OldVoters: []string{"follower-A", "leader-A"},
		},
	}

	response, err := node.InstallSnapshot(req)
	if err != nil {
		t.Fatalf("install snapshot: %v", err)
	}

	if !response.Success {
		t.Fatal("expected successful snapshot installation")
	}

	if response.Term != 4 {
		t.Fatalf("expected response term 4, got %d", response.Term)
	}

	if !reflect.DeepEqual(
		stateMachine.restored,
		[]byte("restored-state"),
	) {
		t.Fatalf(
			"unexpected restored state: %q",
			stateMachine.restored,
		)
	}

	node.mu.RLock()
	defer node.mu.RUnlock()

	if node.snapshotIndex != 3 {
		t.Errorf("expected snapshot index 3, got %d", node.snapshotIndex)
	}

	if node.snapshotTerm != 2 {
		t.Errorf("expected snapshot term 2, got %d", node.snapshotTerm)
	}

	if node.currentTerm != 4 {
		t.Errorf("expected current term 4, got %d", node.currentTerm)
	}

	if node.state != StateFollower {
		t.Errorf("expected follower state, got %v", node.state)
	}

	if node.commitIndex != 3 {
		t.Errorf("expected commit index 3, got %d", node.commitIndex)
	}

	if node.lastApplied != 3 {
		t.Errorf("expected last applied 3, got %d", node.lastApplied)
	}

	if len(node.log) != 2 {
		t.Fatalf("expected 2 retained entries, got %d", len(node.log))
	}

	if string(node.log[0].Command) != "entry-4" {
		t.Errorf("unexpected first retained entry: %q", node.log[0].Command)
	}

	if string(node.log[1].Command) != "entry-5" {
		t.Errorf("unexpected second retained entry: %q", node.log[1].Command)
	}

	if node.lastLogIndex != 5 {
		t.Errorf("expected last log index 5, got %d", node.lastLogIndex)
	}

	if node.lastLogTerm != 3 {
		t.Errorf("expected last log term 3, got %d", node.lastLogTerm)
	}

	saved, err := storage.LoadSnapshot()
	if err != nil {
		t.Fatalf("load saved snapshot: %v", err)
	}

	if saved.LastIncludedIndex != 3 {
		t.Errorf("expected persisted snapshot index 3, got %d", saved.LastIncludedIndex)
	}

	hardState, err := storage.LoadHardState()
	if err != nil {
		t.Fatalf("load hard state: %v", err)
	}

	if hardState.CurrentTerm != 4 {
		t.Errorf("expected persisted term 4, got %d", hardState.CurrentTerm)
	}
}

func TestInstallSnapshotDiscardsSuffixWhenBoundaryTermDiffers(
	t *testing.T,
) {
	storage := NewMemoryStorage()
	node, _ := newSnapshotInstallTestNode(t, storage)

	req := InstallSnapshotRequest{
		Term:              4,
		LeaderID:          "leader-A",
		LastIncludedIndex: 3,
		LastIncludedTerm:  1,
		Data:              []byte("new-state"),
		Configuration: ConfigurationState{
			OldVoters: []string{"follower-A", "leader-A"},
		},
	}

	response, err := node.InstallSnapshot(req)
	if err != nil {
		t.Fatalf("install snapshot: %v", err)
	}

	if !response.Success {
		t.Fatal("expected successful snapshot installation")
	}

	node.mu.RLock()
	defer node.mu.RUnlock()

	if len(node.log) != 0 {
		t.Errorf("expected log suffix to be discarded, got %d entries", len(node.log))
	}

	if node.lastLogIndex != 3 {
		t.Errorf("expected last log index 3, got %d", node.lastLogIndex)
	}

	if node.lastLogTerm != 1 {
		t.Errorf("expected last log term 1, got %d", node.lastLogTerm)
	}
}

func TestInstallSnapshotRejectsOutdatedLeader(
	t *testing.T,
) {
	node, _ := newSnapshotInstallTestNode(t, NewMemoryStorage())

	node.mu.Lock()
	node.currentTerm = 5
	node.mu.Unlock()

	response, err := node.InstallSnapshot(InstallSnapshotRequest{
		Term:              4,
		LeaderID:          "old-leader",
		LastIncludedIndex: 3,
		LastIncludedTerm:  2,
		Data:              []byte("old-state"),
		Configuration: ConfigurationState{
			OldVoters: []string{"follower-A", "leader-A"},
		},
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if response.Success {
		t.Fatal("outdated leader snapshot must be rejected")
	}

	if response.Term != 5 {
		t.Errorf("expected response term 5, got %d", response.Term)
	}
}

func TestInstallSnapshotRejectsInvalidSnapshot(
	t *testing.T,
) {
	node, _ := newSnapshotInstallTestNode(t, NewMemoryStorage())

	_, err := node.InstallSnapshot(InstallSnapshotRequest{
		Term:              4,
		LeaderID:          "leader-A",
		LastIncludedIndex: 3,
		LastIncludedTerm:  0,
		Data:              []byte("invalid"),
		Configuration: ConfigurationState{
			OldVoters: []string{"follower-A", "leader-A"},
		},
	})

	if !errors.Is(err, ErrInvalidInstallSnapshot) {
		t.Fatalf("expected invalid request error, got %v", err)
	}
}

func TestInstallSnapshotPersistenceFailureDoesNotChangeLog(
	t *testing.T,
) {
	baseStorage := NewMemoryStorage()

	storage := &snapshotInstallTestStorage{
		MemoryStorage: baseStorage,
		snapshotErr:   errInstallSnapshotPersistence,
	}

	node, stateMachine := newSnapshotInstallTestNode(t, storage)

	before := append([]LogEntry(nil), node.log...)

	_, err := node.InstallSnapshot(InstallSnapshotRequest{
		Term:              4,
		LeaderID:          "leader-A",
		LastIncludedIndex: 3,
		LastIncludedTerm:  2,
		Data:              []byte("new-state"),
		Configuration: ConfigurationState{
			OldVoters: []string{"follower-A", "leader-A"},
		},
	})

	if !errors.Is(err, errInstallSnapshotPersistence) {
		t.Fatalf("expected persistence error, got %v", err)
	}

	node.mu.RLock()
	defer node.mu.RUnlock()

	if !reflect.DeepEqual(node.log, before) {
		t.Error("log changed after snapshot persistence failure")
	}

	if node.snapshotIndex != 0 {
		t.Errorf("expected snapshot index 0, got %d", node.snapshotIndex)
	}

	if !reflect.DeepEqual(
		stateMachine.restored,
		[]byte(nil),
	) {
		t.Errorf(
			"state machine was unexpectedly restored: %q",
			stateMachine.restored,
		)
	}

}

func TestInstallSnapshotRestoreFailureDoesNotChangeSnapshotMetadata(
	t *testing.T,
) {
	storage := NewMemoryStorage()
	node, stateMachine := newSnapshotInstallTestNode(t, storage)

	stateMachine.err = errInstallSnapshotRestore

	persisted, err := storage.LoadSnapshot()
	if err != nil {
		t.Fatalf("load persisted snapshot: %v", err)
	}

	if persisted.LastIncludedIndex != 0 {
		t.Errorf(
			"expected persisted snapshot index 0 after rollback, got %d",
			persisted.LastIncludedIndex,
		)
	}

	_, err = node.InstallSnapshot(InstallSnapshotRequest{
		Term:              4,
		LeaderID:          "leader-A",
		LastIncludedIndex: 3,
		LastIncludedTerm:  2,
		Data:              []byte("new-state"),
		Configuration: ConfigurationState{
			OldVoters: []string{"follower-A", "leader-A"},
		},
	})

	if !errors.Is(err, errInstallSnapshotRestore) {
		t.Fatalf("expected restore error, got %v", err)
	}

	node.mu.RLock()
	defer node.mu.RUnlock()

	if node.snapshotIndex != 0 {
		t.Errorf("expected snapshot index 0, got %d", node.snapshotIndex)
	}

	if len(node.log) != 5 {
		t.Errorf("expected original 5 log entries, got %d", len(node.log))
	}
}

func TestInstallSnapshotHardStatePersistenceFailurePreservesTerm(
	t *testing.T,
) {
	baseStorage := NewMemoryStorage()

	storage := &snapshotInstallTestStorage{
		MemoryStorage: baseStorage,
		hardStateErr:  errInstallHardStatePersistence,
	}

	node, _ := newSnapshotInstallTestNode(t, storage)

	_, err := node.InstallSnapshot(InstallSnapshotRequest{
		Term:              4,
		LeaderID:          "leader-A",
		LastIncludedIndex: 3,
		LastIncludedTerm:  2,
		Data:              []byte("new-state"),
		Configuration: ConfigurationState{
			OldVoters: []string{"follower-A", "leader-A"},
		},
	})

	if !errors.Is(err, errInstallHardStatePersistence) {
		t.Fatalf("expected hard state persistence error, got %v", err)
	}

	node.mu.RLock()
	defer node.mu.RUnlock()

	if node.currentTerm != 0 {
		t.Errorf("expected current term 0, got %d", node.currentTerm)
	}

	if node.snapshotIndex != 0 {
		t.Errorf("expected snapshot index 0, got %d", node.snapshotIndex)
	}
}

func TestInstallSnapshotDoesNotRollbackSnapshotWhenLogRollbackFails(
	t *testing.T,
) {
	storage := &snapshotInstallTestStorage{
		MemoryStorage: NewMemoryStorage(),
		failLogSaveAt: 2,
	}

	node, stateMachine := newSnapshotInstallTestNode(t, storage)
	stateMachine.err = errInstallSnapshotRestore

	response, err := node.InstallSnapshot(InstallSnapshotRequest{
		Term:              4,
		LeaderID:          "leader-A",
		LastIncludedIndex: 3,
		LastIncludedTerm:  2,
		Data:              []byte("new-state"),
		Configuration: ConfigurationState{
			OldVoters: []string{"follower-A", "leader-A"},
		},
	})

	if err == nil {
		t.Fatal("expected snapshot installation error")
	}

	if !errors.Is(err, errInstallSnapshotRestore) {
		t.Errorf("expected restore error, got %v", err)
	}

	if !errors.Is(err, errStorageUnavailable) {
		t.Errorf("expected log rollback error, got %v", err)
	}

	if response.Success {
		t.Fatal("snapshot installation must not report success")
	}

	if storage.logSaveCalls != 2 {
		t.Errorf(
			"expected 2 log save calls, got %d",
			storage.logSaveCalls,
		)
	}

	// The old snapshot must not be restored because log rollback failed.
	if storage.snapshotSaveCalls != 1 {
		t.Errorf(
			"expected exactly 1 snapshot save call, got %d",
			storage.snapshotSaveCalls,
		)
	}

	persistedSnapshot, loadErr := storage.LoadSnapshot()
	if loadErr != nil {
		t.Fatalf("load persisted snapshot: %v", loadErr)
	}

	if persistedSnapshot.LastIncludedIndex != 3 {
		t.Errorf(
			"expected incoming snapshot index 3 to remain persisted, got %d",
			persistedSnapshot.LastIncludedIndex,
		)
	}

	persistedLog, loadErr := storage.LoadLog()
	if loadErr != nil {
		t.Fatalf("load persisted log: %v", loadErr)
	}

	if persistedLog.FirstIndex != 4 {
		t.Errorf(
			"expected retained log to start at index 4, got %d",
			persistedLog.FirstIndex,
		)
	}

	// Failed installation must not publish the new metadata in memory.
	node.mu.RLock()
	defer node.mu.RUnlock()

	if node.snapshotIndex != 0 {
		t.Errorf(
			"expected in-memory snapshot index 0, got %d",
			node.snapshotIndex,
		)
	}

	if len(node.log) != 5 {
		t.Errorf(
			"expected original 5 in-memory entries, got %d",
			len(node.log),
		)
	}
}

func TestInstallSnapshotRestoresConfiguration(t *testing.T) {
	storage := NewMemoryStorage()
	node, _ := newSnapshotInstallTestNode(t, storage)

	configuration := ConfigurationState{
		OldVoters: []string{"follower-A", "leader-A", "node-C"},
	}

	response, err := node.InstallSnapshot(InstallSnapshotRequest{
		Term:              4,
		LeaderID:          "leader-A",
		LastIncludedIndex: 3,
		LastIncludedTerm:  2,
		Data:              []byte("restored-state"),
		Configuration:     configuration,
	})
	if err != nil {
		t.Fatalf("install snapshot: %v", err)
	}

	if !response.Success {
		t.Fatal("expected successful snapshot installation")
	}

	node.mu.RLock()
	got := node.configuration.Clone()
	node.mu.RUnlock()

	if !equalVoterSets(got.OldVoters, configuration.OldVoters) {
		t.Fatalf("configuration = %+v, want %+v", got, configuration)
	}

	expectedPeers := []string{"leader-A", "node-C"}
	if !reflect.DeepEqual(node.Peers(), expectedPeers) {
		t.Fatalf("peers = %v, want %v", node.Peers(), expectedPeers)
	}
}

func TestInstallSnapshotRejectsConflictingBoundaryConfiguration(
	t *testing.T,
) {
	storage := NewMemoryStorage()
	node, _ := newSnapshotInstallTestNode(t, storage)

	conflicting := ConfigurationState{
		OldVoters: []string{"follower-A", "leader-A", "node-C"},
	}

	if err := storage.SaveConfigurationAt(3, conflicting); err != nil {
		t.Fatalf("save indexed configuration: %v", err)
	}

	_, err := node.InstallSnapshot(InstallSnapshotRequest{
		Term:              4,
		LeaderID:          "leader-A",
		LastIncludedIndex: 3,
		LastIncludedTerm:  2,
		Data:              []byte("new-state"),
		Configuration: ConfigurationState{
			OldVoters: []string{"follower-A", "leader-A"},
		},
	})

	if !errors.Is(err, ErrConfigurationIndexConflict) {
		t.Fatalf(
			"expected configuration conflict, got %v",
			err,
		)
	}

	saved, err := storage.LoadSnapshot()
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}

	if saved.LastIncludedIndex != 0 {
		t.Errorf(
			"snapshot was persisted despite configuration conflict: index %d",
			saved.LastIncludedIndex,
		)
	}
}

func TestInstallSnapshotRejectsConfigurationWithoutRetainedLogEntry(
	t *testing.T,
) {
	storage := NewMemoryStorage()
	node, _ := newSnapshotInstallTestNode(t, storage)

	laterConfiguration := ConfigurationState{
		OldVoters: []string{"follower-A", "leader-A", "node-C"},
	}

	if err := storage.SaveConfigurationAt(4, laterConfiguration); err != nil {
		t.Fatalf("save indexed configuration: %v", err)
	}

	_, err := node.InstallSnapshot(InstallSnapshotRequest{
		Term:              4,
		LeaderID:          "leader-A",
		LastIncludedIndex: 3,
		LastIncludedTerm:  2,
		Data:              []byte("new-state"),
		Configuration: ConfigurationState{
			OldVoters: []string{"follower-A", "leader-A"},
		},
	})

	if !errors.Is(err, ErrConfigurationIndexConflict) {
		t.Fatalf(
			"expected configuration conflict, got %v",
			err,
		)
	}

	saved, err := storage.LoadSnapshot()
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}

	if saved.LastIncludedIndex != 0 {
		t.Errorf(
			"snapshot was persisted despite configuration conflict: index %d",
			saved.LastIncludedIndex,
		)
	}
}
