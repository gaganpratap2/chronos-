package raft

import (
	"path/filepath"
	"testing"
)

func TestInstalledSnapshotRestoresAfterDiskRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-state.log")

	// Create persistent storage.
	storage, err := NewDiskStorage(path)
	if err != nil {
		t.Fatalf("create disk storage: %v", err)
	}

	// Create the original follower.
	node, err := NewNodeWithStorage("follower-A", nil, storage)
	if err != nil {
		t.Fatalf("create follower: %v", err)
	}

	// Install a snapshot.
	response, err := node.InstallSnapshot(InstallSnapshotRequest{
		Term:              4,
		LeaderID:          "leader-A",
		LastIncludedIndex: 3,
		LastIncludedTerm:  2,
		Data:              []byte("recovered-state"),
		Configuration: ConfigurationState{
			OldVoters: []string{"follower-A", "leader-A"},
		},
	})
	if err != nil {
		t.Fatalf("install snapshot: %v", err)
	}
	if !response.Success {
		t.Fatal("snapshot installation was not successful")
	}

	// Close storage to simulate a restart.
	if err := storage.Close(); err != nil {
		t.Fatalf("close original storage: %v", err)
	}

	// Reopen the same persistent storage.
	reopened, err := NewDiskStorage(path)
	if err != nil {
		t.Fatalf("reopen disk storage: %v", err)
	}
	defer reopened.Close()

	// Create a completely new Raft node.
	restarted, err := NewNodeWithStorage("follower-A", nil, reopened)
	if err != nil {
		t.Fatalf("create restarted follower: %v", err)
	}

	// Register a fresh state machine.
	stateMachine := &snapshotTestStateMachine{
		testStateMachine: &testStateMachine{},
	}
	if err := restarted.SetStateMachine(stateMachine); err != nil {
		t.Fatalf("restore state machine: %v", err)
	}

	// Verify recovered state.
	if string(stateMachine.restored) != "recovered-state" {
		t.Fatalf(
			"restored state = %q, want %q",
			stateMachine.restored,
			"recovered-state",
		)
	}

	if restarted.LastApplied() != 3 {
		t.Fatalf("last applied = %d, want 3", restarted.LastApplied())
	}

	restarted.mu.RLock()
	snapshotIndex := restarted.snapshotIndex
	snapshotTerm := restarted.snapshotTerm
	currentTerm := restarted.currentTerm
	restarted.mu.RUnlock()

	if snapshotIndex != 3 {
		t.Errorf("snapshot index = %d, want 3", snapshotIndex)
	}
	if snapshotTerm != 2 {
		t.Errorf("snapshot term = %d, want 2", snapshotTerm)
	}
	if currentTerm != 4 {
		t.Errorf("current term = %d, want 4", currentTerm)
	}
}

func TestSnapshotRecoveryAfterCrashBeforeLogCompaction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-state.log")

	storage, err := NewDiskStorage(path)
	if err != nil {
		t.Fatalf("create disk storage: %v", err)
	}

	// Persist the old log before snapshot creation.
	oldLog := LogState{
		FirstIndex: 1,
		Entries: []LogEntry{
			{Term: 1, Command: []byte("task-1")},
			{Term: 1, Command: []byte("task-2")},
			{Term: 2, Command: []byte("task-3")},
			{Term: 2, Command: []byte("task-4")},
		},
	}

	if err := storage.SaveLog(oldLog); err != nil {
		t.Fatalf("save old log: %v", err)
	}

	// Simulate the crash window: the snapshot is persisted,
	// but the compacted log has not yet been saved.
	snapshot := Snapshot{
		LastIncludedIndex: 2,
		LastIncludedTerm:  1,
		Data:              []byte("snapshot-at-task-2"),
		Configuration: ConfigurationState{
			OldVoters: []string{"node-A", "node-B", "node-C"},
		},
	}

	if err := storage.SaveSnapshot(snapshot); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}

	if err := storage.Close(); err != nil {
		t.Fatalf("close storage: %v", err)
	}

	// Reopen storage as if the process had restarted.
	reopened, err := NewDiskStorage(path)
	if err != nil {
		t.Fatalf("reopen storage: %v", err)
	}
	defer reopened.Close()

	restarted, err := NewNodeWithStorage("node-A", nil, reopened)
	if err != nil {
		t.Fatalf("recover node: %v", err)
	}

	// The snapshot covers indexes 1 and 2. Only indexes 3 and 4
	// should remain in the recovered log.
	restarted.mu.RLock()
	defer restarted.mu.RUnlock()

	if restarted.snapshotIndex != 2 {
		t.Fatalf(
			"snapshot index = %d, want 2",
			restarted.snapshotIndex,
		)
	}

	if restarted.snapshotTerm != 1 {
		t.Fatalf(
			"snapshot term = %d, want 1",
			restarted.snapshotTerm,
		)
	}

	if restarted.lastLogIndex != 4 {
		t.Fatalf(
			"last log index = %d, want 4",
			restarted.lastLogIndex,
		)
	}

	if len(restarted.log) != 2 {
		t.Fatalf(
			"retained log length = %d, want 2",
			len(restarted.log),
		)
	}

	if string(restarted.log[0].Command) != "task-3" ||
		string(restarted.log[1].Command) != "task-4" {
		t.Fatalf(
			"recovered log = %q, %q; want task-3, task-4",
			restarted.log[0].Command,
			restarted.log[1].Command,
		)
	}
}

func TestSnapshotRecoveryDiscardsDivergentOldLogAfterInstallCrash(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-state.log")

	storage, err := NewDiskStorage(path)
	if err != nil {
		t.Fatalf("create disk storage: %v", err)
	}

	oldLog := LogState{
		FirstIndex: 1,
		Entries: []LogEntry{
			{Term: 1, Command: []byte("old-task-1")},
			{Term: 1, Command: []byte("old-task-2")},
			{Term: 2, Command: []byte("old-task-3")},
			{Term: 2, Command: []byte("old-task-4")},
		},
	}

	if err := storage.SaveLog(oldLog); err != nil {
		t.Fatalf("save old log: %v", err)
	}

	// Simulate snapshot installation crashing after the snapshot
	// was persisted but before the divergent old log was replaced.
	snapshot := Snapshot{
		LastIncludedIndex: 2,
		LastIncludedTerm:  3,
		Data:              []byte("installed-leader-state"),
		Configuration: ConfigurationState{
			OldVoters: []string{"node-A", "leader-A"},
		},
	}

	if err := storage.SaveSnapshotInstallMarker(SnapshotInstallMarker{
		LastIncludedIndex: snapshot.LastIncludedIndex,
		LastIncludedTerm:  snapshot.LastIncludedTerm,
	}); err != nil {
		t.Fatalf("save snapshot install marker: %v", err)
	}

	if err := storage.SaveSnapshot(snapshot); err != nil {
		t.Fatalf("save installed snapshot: %v", err)
	}

	if err := storage.Close(); err != nil {
		t.Fatalf("close storage: %v", err)
	}

	reopened, err := NewDiskStorage(path)
	if err != nil {
		t.Fatalf("reopen storage: %v", err)
	}
	defer reopened.Close()

	restarted, err := NewNodeWithStorage("node-A", nil, reopened)
	if err != nil {
		t.Fatalf("recover node after interrupted installation: %v", err)
	}

	restarted.mu.RLock()
	defer restarted.mu.RUnlock()

	if restarted.snapshotIndex != 2 {
		t.Errorf("snapshot index = %d, want 2", restarted.snapshotIndex)
	}

	if restarted.snapshotTerm != 3 {
		t.Errorf("snapshot term = %d, want 3", restarted.snapshotTerm)
	}

	if restarted.lastLogIndex != 2 {
		t.Errorf("last log index = %d, want 2", restarted.lastLogIndex)
	}

	if restarted.lastLogTerm != 3 {
		t.Errorf("last log term = %d, want 3", restarted.lastLogTerm)
	}

	if len(restarted.log) != 0 {
		t.Errorf("retained log length = %d, want 0", len(restarted.log))
	}
}
