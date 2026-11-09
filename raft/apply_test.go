package raft

import (
	"errors"
	"reflect"
	"testing"
)

var errApplyFailed = errors.New("state machine application failed")

type testStateMachine struct {
	applied  []uint64
	commands [][]byte
	failAt   uint64
	failOnce bool
}

func (s *testStateMachine) Apply(
	index uint64,
	command []byte,
) error {
	if s.failOnce && index == s.failAt {
		s.failOnce = false
		return errApplyFailed
	}

	s.applied = append(s.applied, index)
	s.commands = append(s.commands, append([]byte(nil), command...))

	return nil
}

func newApplyTestNode(t *testing.T, entries []LogEntry, commitIndex uint64) *Node {
	t.Helper()

	node, err := NewNode("node-A", nil)
	if err != nil {
		t.Fatal(err)
	}

	node.mu.Lock()
	node.log = entries
	node.commitIndex = commitIndex
	node.lastLogIndex = uint64(len(entries))

	if len(entries) > 0 {
		node.lastLogTerm = entries[len(entries)-1].Term
	}
	node.mu.Unlock()

	return node
}

func TestApplyCommittedEntriesInOrder(t *testing.T) {
	entries := []LogEntry{
		{Term: 1, Command: []byte("task-1")},
		{Term: 1, Command: []byte("task-2")},
		{Term: 1, Command: []byte("task-3")},
		{Term: 1, Command: []byte("task-4")},
	}

	node := newApplyTestNode(t, entries, 3)
	stateMachine := &testStateMachine{}

	if err := node.ApplyCommittedEntries(stateMachine); err != nil {
		t.Fatal(err)
	}

	expected := []uint64{1, 2, 3}

	if !reflect.DeepEqual(stateMachine.applied, expected) {
		t.Fatalf(
			"expected applied indexes %v, got %v",
			expected,
			stateMachine.applied,
		)
	}

	if node.LastApplied() != 3 {
		t.Fatalf("expected lastApplied 3, got %d", node.LastApplied())
	}

	if len(stateMachine.commands) != 3 {
		t.Fatalf(
			"expected 3 commands, got %d",
			len(stateMachine.commands),
		)
	}
}

func TestApplyCommittedEntriesDoesNotApplyUncommittedEntries(t *testing.T) {
	entries := []LogEntry{
		{Term: 1, Command: []byte("task-1")},
		{Term: 1, Command: []byte("task-2")},
		{Term: 1, Command: []byte("task-3")},
	}

	node := newApplyTestNode(t, entries, 2)
	stateMachine := &testStateMachine{}

	if err := node.ApplyCommittedEntries(stateMachine); err != nil {
		t.Fatal(err)
	}

	expected := []uint64{1, 2}

	if !reflect.DeepEqual(stateMachine.applied, expected) {
		t.Fatalf(
			"expected indexes %v, got %v",
			expected,
			stateMachine.applied,
		)
	}
}

func TestApplyCommittedEntriesRetriesAfterFailure(t *testing.T) {
	entries := []LogEntry{
		{Term: 1, Command: []byte("task-1")},
		{Term: 1, Command: []byte("task-2")},
	}

	node := newApplyTestNode(t, entries, 2)

	stateMachine := &testStateMachine{
		failAt:   2,
		failOnce: true,
	}

	err := node.ApplyCommittedEntries(stateMachine)

	if !errors.Is(err, errApplyFailed) {
		t.Fatalf("expected application failure, got %v", err)
	}

	if node.LastApplied() != 1 {
		t.Fatalf(
			"expected lastApplied 1 after failure, got %d",
			node.LastApplied(),
		)
	}

	if err := node.ApplyCommittedEntries(stateMachine); err != nil {
		t.Fatal(err)
	}

	expected := []uint64{1, 2}

	if !reflect.DeepEqual(stateMachine.applied, expected) {
		t.Fatalf(
			"expected indexes %v after retry, got %v",
			expected,
			stateMachine.applied,
		)
	}

	if node.LastApplied() != 2 {
		t.Fatalf("expected lastApplied 2, got %d", node.LastApplied())
	}
}

func TestApplyCommittedEntriesDoesNotDuplicateCompletedEntries(t *testing.T) {
	entries := []LogEntry{
		{Term: 1, Command: []byte("task-1")},
		{Term: 1, Command: []byte("task-2")},
	}

	node := newApplyTestNode(t, entries, 2)
	stateMachine := &testStateMachine{}

	if err := node.ApplyCommittedEntries(stateMachine); err != nil {
		t.Fatal(err)
	}

	if err := node.ApplyCommittedEntries(stateMachine); err != nil {
		t.Fatal(err)
	}

	expected := []uint64{1, 2}

	if !reflect.DeepEqual(stateMachine.applied, expected) {
		t.Fatalf(
			"expected indexes %v, got %v",
			expected,
			stateMachine.applied,
		)
	}
}

func TestApplyCommittedEntriesRejectsNilStateMachine(t *testing.T) {
	node := newApplyTestNode(t, nil, 0)

	err := node.ApplyCommittedEntries(nil)

	if !errors.Is(err, ErrNilStateMachine) {
		t.Fatalf("expected ErrNilStateMachine, got %v", err)
	}
}

func TestApplyCommittedEntriesDetectsMissingCommittedEntry(t *testing.T) {
	node := newApplyTestNode(t, nil, 1)
	stateMachine := &testStateMachine{}

	err := node.ApplyCommittedEntries(stateMachine)

	if !errors.Is(err, ErrCommittedEntryMissing) {
		t.Fatalf("expected ErrCommittedEntryMissing, got %v", err)
	}
}
