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
