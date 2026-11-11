package raft

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"
)

func TestNewNodePersistsInitialConfiguration(t *testing.T) {
	storage := NewMemoryStorage()

	node, err := NewNodeWithStorage(
		"node-A",
		[]string{"node-B", "node-C"},
		storage,
	)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	want := ConfigurationState{
		OldVoters: []string{"node-A", "node-B", "node-C"},
	}

	got, err := storage.LoadConfiguration()
	if err != nil {
		t.Fatalf("load configuration: %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("persisted configuration = %+v, want %+v", got, want)
	}

	if !reflect.DeepEqual(node.configuration, want) {
		t.Fatalf("node configuration = %+v, want %+v", node.configuration, want)
	}
}

func TestNewNodeRestoresPersistedConfigurationAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft", "hard-state.log")

	firstStorage, err := NewDiskStorage(path)
	if err != nil {
		t.Fatalf("create first storage: %v", err)
	}

	want := ConfigurationState{
		OldVoters: []string{"node-A", "node-C", "node-D"},
	}

	if err := firstStorage.SaveSnapshot(Snapshot{
		LastIncludedIndex: 1,
		LastIncludedTerm:  1,
		Configuration:     want.Clone(),
		Data:              []byte("state"),
	}); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}

	if err := firstStorage.Close(); err != nil {
		t.Fatalf("close first storage: %v", err)
	}

	secondStorage, err := NewDiskStorage(path)
	if err != nil {
		t.Fatalf("reopen storage: %v", err)
	}
	defer secondStorage.Close()

	// These peers are outdated. Persisted membership must win.
	node, err := NewNodeWithStorage(
		"node-A",
		[]string{"node-B", "node-C"},
		secondStorage,
	)
	if err != nil {
		t.Fatalf("restore node: %v", err)
	}

	if !reflect.DeepEqual(node.configuration, want) {
		t.Fatalf(
			"restored configuration = %+v, want %+v",
			node.configuration,
			want,
		)
	}

	wantPeers := []string{"node-C", "node-D"}

	if !reflect.DeepEqual(node.Peers(), wantPeers) {
		t.Fatalf("restored peers = %v, want %v", node.Peers(), wantPeers)
	}
}

func TestNewNodeRestoresJointConfigurationAndPeerUnion(t *testing.T) {
	storage := NewMemoryStorage()

	want := ConfigurationState{
		OldVoters: []string{"node-A", "node-B", "node-C"},
		NewVoters: []string{"node-A", "node-C", "node-D"},
	}

	if err := storage.SaveSnapshot(Snapshot{
		LastIncludedIndex: 1,
		LastIncludedTerm:  1,
		Configuration:     want.Clone(),
		Data:              []byte("state"),
	}); err != nil {
		t.Fatalf("save joint configuration snapshot: %v", err)
	}

	node, err := NewNodeWithStorage(
		"node-A",
		[]string{"node-B"},
		storage,
	)
	if err != nil {
		t.Fatalf("restore node: %v", err)
	}

	if !reflect.DeepEqual(node.configuration, want) {
		t.Fatalf(
			"restored configuration = %+v, want %+v",
			node.configuration,
			want,
		)
	}

	wantPeers := []string{"node-B", "node-C", "node-D"}

	if !reflect.DeepEqual(node.Peers(), wantPeers) {
		t.Fatalf("restored peers = %v, want %v", node.Peers(), wantPeers)
	}
}

func TestNewNodeRejectsInvalidPersistedConfiguration(t *testing.T) {
	storage := NewMemoryStorage()

	// Simulate corrupted persisted state without using SaveConfiguration,
	// which correctly rejects invalid configurations.
	storage.configuration = ConfigurationState{
		OldVoters: []string{"node-A", "node-A"},
	}

	_, err := NewNodeWithStorage(
		"node-A",
		[]string{"node-B"},
		storage,
	)

	if !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf(
			"error = %v, want ErrInvalidConfiguration",
			err,
		)
	}
}

func TestNewNodeRestoresIndexedConfigurationFromLog(t *testing.T) {
	storage := NewMemoryStorage()

	joint := ConfigurationState{
		OldVoters: []string{"node-A", "node-B"},
		NewVoters: []string{"node-A", "node-C"},
	}

	if err := storage.SaveLog(LogState{
		FirstIndex: 1,
		Entries: []LogEntry{
			{
				Term:          1,
				Type:          LogEntryConfiguration,
				Configuration: &joint,
			},
		},
	}); err != nil {
		t.Fatalf("save log: %v", err)
	}

	if err := storage.SaveConfigurationAt(1, joint); err != nil {
		t.Fatalf("save indexed configuration: %v", err)
	}

	node, err := NewNodeWithStorage(
		"node-A",
		[]string{"node-B"},
		storage,
	)
	if err != nil {
		t.Fatalf("restore node: %v", err)
	}

	if !reflect.DeepEqual(node.configuration, joint) {
		t.Fatalf(
			"configuration = %+v, want %+v",
			node.configuration,
			joint,
		)
	}

	wantPeers := []string{"node-B", "node-C"}
	if !reflect.DeepEqual(node.Peers(), wantPeers) {
		t.Fatalf("peers = %v, want %v", node.Peers(), wantPeers)
	}
}

func TestNewNodeRejectsIndexedConfigurationLogMismatch(t *testing.T) {
	storage := NewMemoryStorage()

	recorded := ConfigurationState{
		OldVoters: []string{"node-A", "node-B"},
		NewVoters: []string{"node-A", "node-C"},
	}

	logged := ConfigurationState{
		OldVoters: []string{"node-A", "node-B"},
		NewVoters: []string{"node-A", "node-D"},
	}

	if err := storage.SaveLog(LogState{
		FirstIndex: 1,
		Entries: []LogEntry{
			{
				Term:          1,
				Type:          LogEntryConfiguration,
				Configuration: &logged,
			},
		},
	}); err != nil {
		t.Fatalf("save log: %v", err)
	}

	if err := storage.SaveConfigurationAt(1, recorded); err != nil {
		t.Fatalf("save indexed configuration: %v", err)
	}

	_, err := NewNodeWithStorage(
		"node-A",
		[]string{"node-B"},
		storage,
	)

	if !errors.Is(err, ErrConfigurationIndexConflict) {
		t.Fatalf(
			"error = %v, want ErrConfigurationIndexConflict",
			err,
		)
	}
}
