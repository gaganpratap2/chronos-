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

func TestApplyConfigurationWithoutStateMachine(t *testing.T) {
	node := newApplyTestNode(t, []LogEntry{
		{
			Term: 1,
			Type: LogEntryConfiguration,
			Configuration: &ConfigurationState{
				OldVoters: []string{"node-A"},
				NewVoters: []string{"node-A", "node-B"},
			},
		},
		{
			Term: 1,
			Type: LogEntryConfiguration,
			Configuration: &ConfigurationState{
				OldVoters: []string{"node-A", "node-B"},
			},
		},
	}, 2)

	if err := node.applyCommittedEntriesIfConfigured(); err != nil {
		t.Fatal(err)
	}

	want := ConfigurationState{
		OldVoters: []string{"node-A", "node-B"},
	}

	if !reflect.DeepEqual(node.configuration, want) {
		t.Fatalf("configuration = %+v, want %+v", node.configuration, want)
	}

	if node.LastApplied() != 2 {
		t.Fatalf("LastApplied() = %d, want 2", node.LastApplied())
	}
}

func TestCommandWaitsUntilStateMachineIsRegistered(t *testing.T) {
	node := newApplyTestNode(t, []LogEntry{
		{
			Term: 1,
			Type: LogEntryConfiguration,
			Configuration: &ConfigurationState{
				OldVoters: []string{"node-A"},
				NewVoters: []string{"node-A", "node-B"},
			},
		},
		{
			Term: 1,
			Type: LogEntryConfiguration,
			Configuration: &ConfigurationState{
				OldVoters: []string{"node-A", "node-B"},
			},
		},
		{
			Term:    1,
			Command: []byte("create-job"),
		},
	}, 3)

	if err := node.applyCommittedEntriesIfConfigured(); err != nil {
		t.Fatal(err)
	}

	if node.LastApplied() != 2 {
		t.Fatalf(
			"LastApplied() = %d, want 2 while command is waiting",
			node.LastApplied(),
		)
	}

	stateMachine := &testStateMachine{}

	if err := node.SetStateMachine(stateMachine); err != nil {
		t.Fatal(err)
	}

	if node.LastApplied() != 3 {
		t.Fatalf("LastApplied() = %d, want 3", node.LastApplied())
	}

	if !reflect.DeepEqual(stateMachine.applied, []uint64{3}) {
		t.Fatalf(
			"applied indexes = %v, want [3]",
			stateMachine.applied,
		)
	}
}

func TestApplyConfigurationUpdatesReplicationProgress(t *testing.T) {
	node := newApplyTestNode(t, []LogEntry{
		{
			Term: 1,
			Type: LogEntryConfiguration,
			Configuration: &ConfigurationState{
				OldVoters: []string{"node-A"},
				NewVoters: []string{"node-A", "node-C", "node-D"},
			},
		},
		{
			Term: 1,
			Type: LogEntryConfiguration,
			Configuration: &ConfigurationState{
				OldVoters: []string{"node-A", "node-C", "node-D"},
			},
		},
	}, 2)

	node.mu.Lock()
	node.nextIndex = map[string]uint64{
		"node-B": 4,
		"node-C": 3,
	}

	node.matchIndex = map[string]uint64{
		"node-A": 1,
		"node-B": 3,
		"node-C": 2,
	}
	node.mu.Unlock()

	if err := node.ApplyCommittedEntries(&testStateMachine{}); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(
		node.Peers(),
		[]string{"node-C", "node-D"},
	) {
		t.Fatalf("Peers() = %v, want [node-C node-D]", node.Peers())
	}

	node.mu.RLock()
	defer node.mu.RUnlock()

	if got := node.nextIndex["node-C"]; got != 3 {
		t.Errorf("nextIndex[node-C] = %d, want 3", got)
	}

	if got := node.matchIndex["node-C"]; got != 2 {
		t.Errorf("matchIndex[node-C] = %d, want 2", got)
	}

	if got := node.nextIndex["node-D"]; got != 3 {
		t.Errorf("nextIndex[node-D] = %d, want 2", got)
	}

	if got := node.matchIndex["node-D"]; got != 0 {
		t.Errorf("matchIndex[node-D] = %d, want 0", got)
	}

	if _, exists := node.nextIndex["node-B"]; exists {
		t.Error("removed peer node-B remains in nextIndex")
	}

	if _, exists := node.matchIndex["node-B"]; exists {
		t.Error("removed peer node-B remains in matchIndex")
	}
}

type failingConfigurationStorage struct {
	*MemoryStorage
	failConfigurationSave bool
}

func (s *failingConfigurationStorage) SaveConfiguration(
	configuration ConfigurationState,
) error {
	if s.failConfigurationSave {
		return errStorageUnavailable
	}

	return s.MemoryStorage.SaveConfiguration(configuration)
}

func TestApplyConfigurationDoesNotChangeStateWhenPersistenceFails(
	t *testing.T,
) {
	storage := &failingConfigurationStorage{
		MemoryStorage: NewMemoryStorage(),
	}

	node, err := NewNodeWithStorage(
		"node-A",
		[]string{"node-B", "node-C"},
		storage,
	)
	if err != nil {
		t.Fatal(err)
	}

	node.mu.Lock()

	node.log = []LogEntry{
		{
			Term: 1,
			Type: LogEntryConfiguration,
			Configuration: &ConfigurationState{
				OldVoters: []string{"node-A", "node-B", "node-C"},
				NewVoters: []string{"node-A", "node-C", "node-D"},
			},
		},
	}

	node.lastLogIndex = 1
	node.lastLogTerm = 1
	node.commitIndex = 1
	node.state = StateLeader

	node.nextIndex = map[string]uint64{
		"node-B": 2,
		"node-C": 3,
	}

	node.matchIndex = map[string]uint64{
		"node-A": 1,
		"node-B": 0,
		"node-C": 2,
	}

	node.mu.Unlock()

	beforeConfiguration := node.configuration.Clone()
	beforePeers := node.Peers()

	beforeNextIndex := map[string]uint64{
		"node-B": 2,
		"node-C": 3,
	}

	beforeMatchIndex := map[string]uint64{
		"node-A": 1,
		"node-B": 0,
		"node-C": 2,
	}

	storage.failConfigurationSave = true

	err = node.ApplyCommittedEntries(&testStateMachine{})

	if !errors.Is(err, errStorageUnavailable) {
		t.Fatalf(
			"ApplyCommittedEntries() error = %v, want storage error",
			err,
		)
	}

	if node.LastApplied() != 0 {
		t.Errorf("LastApplied() = %d, want 0", node.LastApplied())
	}

	node.mu.RLock()
	defer node.mu.RUnlock()

	if !reflect.DeepEqual(node.configuration, beforeConfiguration) {
		t.Errorf(
			"configuration changed: got %+v, want %+v",
			node.configuration,
			beforeConfiguration,
		)
	}

	if !reflect.DeepEqual(node.peers, beforePeers) {
		t.Errorf("peers changed: got %v, want %v", node.peers, beforePeers)
	}

	if !reflect.DeepEqual(node.nextIndex, beforeNextIndex) {
		t.Errorf(
			"nextIndex changed: got %v, want %v",
			node.nextIndex,
			beforeNextIndex,
		)
	}

	if !reflect.DeepEqual(node.matchIndex, beforeMatchIndex) {
		t.Errorf(
			"matchIndex changed: got %v, want %v",
			node.matchIndex,
			beforeMatchIndex,
		)
	}

	if node.state != StateLeader {
		t.Errorf("state = %v, want leader", node.state)
	}

	persisted, loadErr := storage.LoadConfiguration()
	if loadErr != nil {
		t.Fatal(loadErr)
	}

	if !reflect.DeepEqual(persisted, beforeConfiguration) {
		t.Errorf(
			"persisted configuration changed: got %+v, want %+v",
			persisted,
			beforeConfiguration,
		)
	}
}

func (s *failingConfigurationStorage) SaveConfigurationAt(
	index uint64,
	configuration ConfigurationState,
) error {
	if s.failConfigurationSave {
		return errStorageUnavailable
	}

	return s.MemoryStorage.SaveConfigurationAt(
		index,
		configuration,
	)
}

func TestCommittedConfigurationRemovesLocalLeader(t *testing.T) {
	node := newApplyTestNode(t, []LogEntry{
		{
			Term: 1,
			Type: LogEntryConfiguration,
			Configuration: &ConfigurationState{
				OldVoters: []string{"node-A"},
				NewVoters: []string{"node-B", "node-C"},
			},
		},
		{
			Term: 1,
			Type: LogEntryConfiguration,
			Configuration: &ConfigurationState{
				OldVoters: []string{"node-B", "node-C"},
			},
		},
	}, 2)

	node.mu.Lock()
	node.state = StateLeader
	node.mu.Unlock()

	if err := node.ApplyCommittedEntries(&testStateMachine{}); err != nil {
		t.Fatal(err)
	}

	if node.State() != StateFollower {
		t.Fatalf("State() = %v, want follower", node.State())
	}

	if node.LastApplied() != 2 {
		t.Fatalf("LastApplied() = %d, want 2", node.LastApplied())
	}

	if !reflect.DeepEqual(
		node.Peers(),
		[]string{"node-B", "node-C"},
	) {
		t.Fatalf("Peers() = %v, want [node-B node-C]", node.Peers())
	}
}
