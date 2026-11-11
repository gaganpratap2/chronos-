package raft

import (
	"errors"
	"testing"
)

func newProposalTestLeader(
	t *testing.T,
	storage Storage,
	peers []string,
	votes int,
) *Node {
	t.Helper()

	node, err := NewNodeWithStorage("A", peers, storage)
	if err != nil {
		t.Fatal(err)
	}

	if err := node.StartElection(); err != nil {
		t.Fatal(err)
	}

	if err := node.becomeLeaderIfMajority(
		node.CurrentTerm(),
		votes,
	); err != nil {
		t.Fatal(err)
	}

	return node
}

func TestProposeConfigurationPersistsEntry(t *testing.T) {
	storage := NewMemoryStorage()

	node := newProposalTestLeader(
		t,
		storage,
		[]string{"B", "C"},
		2,
	)

	next := ConfigurationState{
		OldVoters: []string{"A", "B", "C"},
		NewVoters: []string{"A", "B", "C", "D"},
	}

	index, err := node.ProposeConfiguration(next)
	if err != nil {
		t.Fatal(err)
	}

	if index != 1 {
		t.Fatalf("proposal index = %d, want 1", index)
	}

	if len(node.log) != 1 {
		t.Fatalf("log length = %d, want 1", len(node.log))
	}

	entry := node.log[0]

	if entry.Type != LogEntryConfiguration {
		t.Fatalf("entry type = %q, want configuration", entry.Type)
	}

	if entry.Configuration == nil {
		t.Fatal("configuration entry has nil configuration")
	}

	if !equalVoterSets(
		entry.Configuration.NewVoters,
		next.NewVoters,
	) {
		t.Fatal("log entry contains incorrect new voters")
	}

	// A three-node cluster cannot commit using only its own copy.
	if node.commitIndex != 0 {
		t.Fatalf("commitIndex = %d, want 0", node.commitIndex)
	}

	// The active configuration must not change before commitment.
	if node.configuration.IsJoint() {
		t.Fatal("active configuration changed before commitment")
	}

	if node.matchIndex["A"] != index {
		t.Fatalf("leader matchIndex = %d, want %d",
			node.matchIndex["A"], index)
	}

	stored, err := storage.LoadLog()
	if err != nil {
		t.Fatal(err)
	}

	if len(stored.Entries) != 1 {
		t.Fatalf("persisted log length = %d, want 1",
			len(stored.Entries))
	}
}

func TestProposeConfigurationAppliesOnSingleNode(t *testing.T) {
	storage := NewMemoryStorage()

	node := newProposalTestLeader(t, storage, nil, 1)

	next := ConfigurationState{
		OldVoters: []string{"A"},
		NewVoters: []string{"A", "B"},
	}

	index, err := node.ProposeConfiguration(next)
	if err != nil {
		t.Fatal(err)
	}

	if index != 1 {
		t.Fatalf("proposal index = %d, want 1", index)
	}

	if node.commitIndex != index {
		t.Fatalf("commitIndex = %d, want %d",
			node.commitIndex, index)
	}

	if !node.configuration.IsJoint() {
		t.Fatal("committed joint configuration was not applied")
	}

	if !equalVoterSets(
		node.configuration.NewVoters,
		next.NewVoters,
	) {
		t.Fatal("applied configuration has incorrect new voters")
	}
}

func TestProposeConfigurationRejectsNonLeader(t *testing.T) {
	node, err := NewNode("A", []string{"B", "C"})
	if err != nil {
		t.Fatal(err)
	}

	_, err = node.ProposeConfiguration(ConfigurationState{
		OldVoters: []string{"A", "B", "C"},
		NewVoters: []string{"A", "B", "C", "D"},
	})

	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("error = %v, want ErrNotLeader", err)
	}
}

func TestProposeConfigurationRejectsInvalidTransition(t *testing.T) {
	node := newProposalTestLeader(
		t,
		NewMemoryStorage(),
		[]string{"B", "C"},
		2,
	)

	// Stable-to-stable changes are not permitted.
	_, err := node.ProposeConfiguration(ConfigurationState{
		OldVoters: []string{"A", "B", "D"},
	})

	if !errors.Is(err, ErrInvalidConfigurationTransition) {
		t.Fatalf("error = %v, want ErrInvalidConfigurationTransition", err)
	}

	if len(node.log) != 0 {
		t.Fatalf("log length = %d, want 0", len(node.log))
	}
}

func TestProposeConfigurationDoesNotPublishOnStorageFailure(
	t *testing.T,
) {
	storage := &failingLogStorage{
		MemoryStorage: NewMemoryStorage(),
		failLogSave:   true,
	}

	node := newProposalTestLeader(
		t,
		storage,
		[]string{"B", "C"},
		2,
	)

	_, err := node.ProposeConfiguration(ConfigurationState{
		OldVoters: []string{"A", "B", "C"},
		NewVoters: []string{"A", "B", "C", "D"},
	})

	if !errors.Is(err, errStorageUnavailable) {
		t.Fatalf("error = %v, want storage error", err)
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

	if node.matchIndex["A"] != 0 {
		t.Fatalf("leader matchIndex = %d, want 0", node.matchIndex["A"])
	}

	if node.configuration.IsJoint() {
		t.Fatal("active configuration changed after storage failure")
	}

	stored, err := storage.LoadLog()
	if err != nil {
		t.Fatal(err)
	}

	if len(stored.Entries) != 0 {
		t.Fatalf("persisted log length = %d, want 0", len(stored.Entries))
	}
}
