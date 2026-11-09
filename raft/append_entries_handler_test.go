package raft

import (
	"bytes"
	"reflect"
	"testing"
)

func newAppendEntriesTestNode(t *testing.T) *Node {
	t.Helper()

	node, err := NewNode("follower-1", []string{"leader-1"})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	return node
}

func TestAppendEntriesRejectsOlderTerm(t *testing.T) {
	node := newAppendEntriesTestNode(t)

	if err := node.ObserveHigherTerm(2); err != nil {
		t.Fatal(err)
	}

	response, err := node.AppendEntries(AppendEntriesRequest{
		Term:     1,
		LeaderID: "leader-1",
	})

	if err != nil {
		t.Fatal(err)
	}

	if response.Success {
		t.Fatal("expected older-term request to be rejected")
	}

	if response.Term != 2 {
		t.Fatalf("expected term 2, got %d", response.Term)
	}
}

func TestAppendEntriesRejectsPreviousLogMismatch(t *testing.T) {
	node := newAppendEntriesTestNode(t)

	response, err := node.AppendEntries(AppendEntriesRequest{
		Term:         1,
		LeaderID:     "leader-1",
		PrevLogIndex: 1,
		PrevLogTerm:  1,
	})

	if err != nil {
		t.Fatal(err)
	}

	if response.Success {
		t.Fatal("expected previous-log mismatch to fail")
	}
}

func TestAppendEntriesReplicatesEntries(t *testing.T) {
	node := newAppendEntriesTestNode(t)

	command := []byte("create-job")

	response, err := node.AppendEntries(AppendEntriesRequest{
		Term:     1,
		LeaderID: "leader-1",
		Entries: []LogEntry{
			{Term: 1, Command: command},
			{Term: 1, Command: []byte("start-job")},
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	if !response.Success {
		t.Fatal("expected entries to be replicated")
	}

	if len(node.log) != 2 {
		t.Fatalf("expected 2 log entries, got %d", len(node.log))
	}

	if node.lastLogIndex != 2 {
		t.Fatalf("expected last log index 2, got %d", node.lastLogIndex)
	}

	if node.lastLogTerm != 1 {
		t.Fatalf("expected last log term 1, got %d", node.lastLogTerm)
	}

	if !bytes.Equal(node.log[0].Command, []byte("create-job")) {
		t.Fatal("unexpected first command")
	}

	// The follower must own a copy of the command.
	command[0] = 'X'

	if !bytes.Equal(node.log[0].Command, []byte("create-job")) {
		t.Fatal("log command changed after caller modified input")
	}
}

func TestAppendEntriesReplacesConflictingEntries(t *testing.T) {
	node := newAppendEntriesTestNode(t)

	node.log = []LogEntry{
		{Term: 1, Command: []byte("first")},
		{Term: 1, Command: []byte("old-second")},
	}
	node.lastLogIndex = 2
	node.lastLogTerm = 1

	response, err := node.AppendEntries(AppendEntriesRequest{
		Term:         2,
		LeaderID:     "leader-1",
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries: []LogEntry{
			{Term: 2, Command: []byte("new-second")},
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	if !response.Success {
		t.Fatal("expected conflicting entry to be replaced")
	}

	if len(node.log) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(node.log))
	}

	if node.log[1].Term != 2 {
		t.Fatalf("expected replacement term 2, got %d", node.log[1].Term)
	}

	if string(node.log[1].Command) != "new-second" {
		t.Fatalf("unexpected replacement command: %s", node.log[1].Command)
	}
}

func TestAppendEntriesHeartbeatAdvancesCommitIndex(t *testing.T) {
	node := newAppendEntriesTestNode(t)

	node.log = []LogEntry{
		{Term: 1, Command: []byte("first")},
		{Term: 1, Command: []byte("second")},
	}
	node.lastLogIndex = 2
	node.lastLogTerm = 1

	response, err := node.AppendEntries(AppendEntriesRequest{
		Term:         1,
		LeaderID:     "leader-1",
		PrevLogIndex: 2,
		PrevLogTerm:  1,
		LeaderCommit: 10,
	})

	if err != nil {
		t.Fatal(err)
	}

	if !response.Success {
		t.Fatal("expected heartbeat to succeed")
	}

	if node.commitIndex != 2 {
		t.Fatalf("expected commit index capped at 2, got %d", node.commitIndex)
	}

	if len(node.log) != 2 {
		t.Fatalf("heartbeat unexpectedly changed log length: %d", len(node.log))
	}
}

func TestAppendEntriesRejectsInvalidEntryTerm(t *testing.T) {
	node := newAppendEntriesTestNode(t)

	response, err := node.AppendEntries(AppendEntriesRequest{
		Term:     1,
		LeaderID: "leader-1",
		Entries: []LogEntry{
			{Term: 2, Command: []byte("invalid")},
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	if response.Success {
		t.Fatal("expected entry from a future term to be rejected")
	}

	if len(node.log) != 0 {
		t.Fatal("invalid entry was appended")
	}
}

func TestFollowerAutomaticallyAppliesCommittedEntries(t *testing.T) {
	node := newAppendEntriesTestNode(t)

	stateMachine := &testStateMachine{}

	if err := node.SetStateMachine(stateMachine); err != nil {
		t.Fatal(err)
	}

	response, err := node.AppendEntries(AppendEntriesRequest{
		Term:     1,
		LeaderID: "leader-1",
		Entries: []LogEntry{
			{Term: 1, Command: []byte("task-1")},
			{Term: 1, Command: []byte("task-2")},
		},
		LeaderCommit: 2,
	})

	if err != nil {
		t.Fatal(err)
	}

	if !response.Success {
		t.Fatal("expected AppendEntries to succeed")
	}

	expected := []uint64{1, 2}

	if !reflect.DeepEqual(stateMachine.applied, expected) {
		t.Fatalf(
			"expected applied indexes %v, got %v",
			expected,
			stateMachine.applied,
		)
	}

	if node.LastApplied() != 2 {
		t.Fatalf("lastApplied = %d, want 2", node.LastApplied())
	}
}
