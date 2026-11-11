package raft

import (
	"errors"
	"testing"
)

type failingLogStorage struct {
	*MemoryStorage

	failLogSave   bool
	failLogSaveAt int
	logSaveCalls  int
}

func (s *failingLogStorage) SaveLog(state LogState) error {
	s.logSaveCalls++

	if s.failLogSave {
		return errStorageUnavailable
	}

	if s.failLogSaveAt != 0 &&
		s.logSaveCalls == s.failLogSaveAt {
		return errStorageUnavailable
	}

	return s.MemoryStorage.SaveLog(state)
}

func TestAppendEntriesDoesNotAcknowledgeWhenLogPersistenceFails(
	t *testing.T,
) {
	storage := &failingLogStorage{
		MemoryStorage: NewMemoryStorage(),
		failLogSave:   true,
	}

	node, err := NewNodeWithStorage(
		"follower-1",
		[]string{"leader-1"},
		storage,
	)
	if err != nil {
		t.Fatal(err)
	}

	response, err := node.AppendEntries(AppendEntriesRequest{
		Term:     1,
		LeaderID: "leader-1",
		Entries: []LogEntry{
			{Term: 1, Command: []byte("create-job")},
		},
		LeaderCommit: 1,
	})

	if !errors.Is(err, errStorageUnavailable) {
		t.Fatalf("AppendEntries() error = %v, want storage error", err)
	}

	if response.Success {
		t.Fatal("follower acknowledged entries despite persistence failure")
	}

	if len(node.log) != 0 {
		t.Fatalf("in-memory log length = %d, want 0", len(node.log))
	}

	if node.lastLogIndex != 0 {
		t.Fatalf("lastLogIndex = %d, want 0", node.lastLogIndex)
	}

	if node.commitIndex != 0 {
		t.Fatalf("commitIndex = %d, want 0", node.commitIndex)
	}

	stored, err := storage.LoadLog()
	if err != nil {
		t.Fatal(err)
	}

	if len(stored.Entries) != 0 {
		t.Fatalf("persisted log length = %d, want 0", len(stored.Entries))
	}
}
