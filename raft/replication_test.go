package raft

import (
	"context"
	"testing"
)

func newReplicationTestCluster(
	t *testing.T,
	leaderEntries []LogEntry,
	followerEntries []LogEntry,
) (*Node, *Node, *InMemoryAppendEntriesTransport) {
	t.Helper()

	leader, err := NewNode("node-A", []string{"node-B"})
	if err != nil {
		t.Fatal(err)
	}

	leader.log = leaderEntries
	leader.lastLogIndex = uint64(len(leaderEntries))

	if len(leaderEntries) > 0 {
		leader.lastLogTerm = leaderEntries[len(leaderEntries)-1].Term
	}

	if err := leader.StartElection(); err != nil {
		t.Fatal(err)
	}

	if err := leader.becomeLeaderIfMajority(1, 2); err != nil {
		t.Fatal(err)
	}

	follower, err := NewNode("node-B", []string{"node-A"})
	if err != nil {
		t.Fatal(err)
	}

	follower.log = followerEntries
	follower.lastLogIndex = uint64(len(followerEntries))

	if len(followerEntries) > 0 {
		follower.lastLogTerm = followerEntries[len(followerEntries)-1].Term
	}

	transport := NewInMemoryAppendEntriesTransport()

	if err := transport.Register("node-B", follower); err != nil {
		t.Fatal(err)
	}

	return leader, follower, transport
}

func TestReplicateToPeerSuccessfullyReplicatesEntries(t *testing.T) {
	leaderEntries := []LogEntry{
		{Term: 1, Command: []byte("task-1")},
		{Term: 1, Command: []byte("task-2")},
	}

	followerEntries := []LogEntry{
		{Term: 1, Command: []byte("task-1")},
	}

	leader, follower, transport := newReplicationTestCluster(
		t,
		leaderEntries,
		followerEntries,
	)

	// The follower already has entry 1.
	// Start replication from entry 2.
	leader.mu.Lock()
	leader.nextIndex["node-B"] = 2
	leader.mu.Unlock()

	ok, err := leader.ReplicateToPeer(
		context.Background(),
		transport,
		"node-B",
	)
	if err != nil {
		t.Fatal(err)
	}

	if !ok {
		t.Fatal("expected successful replication")
	}

	follower.mu.RLock()
	if len(follower.log) != 2 {
		t.Fatalf("follower log length = %d, want 2", len(follower.log))
	}

	if string(follower.log[1].Command) != "task-2" {
		t.Errorf("replicated command = %q, want task-2", follower.log[1].Command)
	}
	follower.mu.RUnlock()

	leader.mu.RLock()
	defer leader.mu.RUnlock()

	if leader.matchIndex["node-B"] != 2 {
		t.Errorf("matchIndex[node-B] = %d, want 2", leader.matchIndex["node-B"])
	}

	if leader.nextIndex["node-B"] != 3 {
		t.Errorf("nextIndex[node-B] = %d, want 3", leader.nextIndex["node-B"])
	}
}

func TestReplicateToPeerSendsHeartbeat(t *testing.T) {
	entries := []LogEntry{
		{Term: 1, Command: []byte("task-1")},
		{Term: 1, Command: []byte("task-2")},
	}

	leader, follower, transport := newReplicationTestCluster(
		t,
		entries,
		entries,
	)

	ok, err := leader.ReplicateToPeer(
		context.Background(),
		transport,
		"node-B",
	)
	if err != nil {
		t.Fatal(err)
	}

	if !ok {
		t.Fatal("expected heartbeat to succeed")
	}

	leader.mu.RLock()
	defer leader.mu.RUnlock()

	if leader.matchIndex["node-B"] != 2 {
		t.Errorf("matchIndex[node-B] = %d, want 2", leader.matchIndex["node-B"])
	}

	follower.mu.RLock()
	defer follower.mu.RUnlock()

	if len(follower.log) != 2 {
		t.Errorf("follower log length = %d, want 2", len(follower.log))
	}
}

func TestReplicateToPeerBacktracksAndRetries(t *testing.T) {
	leaderEntries := []LogEntry{
		{Term: 1, Command: []byte("task-1")},
		{Term: 1, Command: []byte("task-2")},
		{Term: 1, Command: []byte("task-3")},
	}

	followerEntries := []LogEntry{
		{Term: 1, Command: []byte("task-1")},
	}

	leader, follower, transport := newReplicationTestCluster(
		t,
		leaderEntries,
		followerEntries,
	)

	// First attempt: previous index 3 does not exist on the follower.
	ok, err := leader.ReplicateToPeer(
		context.Background(),
		transport,
		"node-B",
	)
	if err != nil {
		t.Fatal(err)
	}

	if ok {
		t.Fatal("expected first replication attempt to fail")
	}

	leader.mu.RLock()
	nextIndex := leader.nextIndex["node-B"]
	leader.mu.RUnlock()

	if nextIndex != 3 {
		t.Fatalf("nextIndex after first failure = %d, want 3", nextIndex)
	}

	// Second attempt: previous index 2 is still missing.
	ok, err = leader.ReplicateToPeer(
		context.Background(),
		transport,
		"node-B",
	)
	if err != nil {
		t.Fatal(err)
	}

	if ok {
		t.Fatal("expected second replication attempt to fail")
	}

	leader.mu.RLock()
	nextIndex = leader.nextIndex["node-B"]
	leader.mu.RUnlock()

	if nextIndex != 2 {
		t.Fatalf("nextIndex after second failure = %d, want 2", nextIndex)
	}

	// Third attempt: previous index 1 matches, so entries 2 and 3 can be sent.
	ok, err = leader.ReplicateToPeer(
		context.Background(),
		transport,
		"node-B",
	)
	if err != nil {
		t.Fatal(err)
	}

	if !ok {
		t.Fatal("expected third replication attempt to succeed")
	}

	follower.mu.RLock()
	defer follower.mu.RUnlock()

	if len(follower.log) != 3 {
		t.Errorf("follower log length = %d, want 3", len(follower.log))
	}
}

func TestReplicateToPeerStepsDownOnHigherTerm(t *testing.T) {
	leader, follower, transport := newReplicationTestCluster(
		t,
		nil,
		nil,
	)

	// Simulate a follower that has already observed a newer term.
	if err := follower.storage.SaveHardState(HardState{
		CurrentTerm: 2,
	}); err != nil {
		t.Fatal(err)
	}

	follower.mu.Lock()
	follower.currentTerm = 2
	follower.mu.Unlock()

	ok, err := leader.ReplicateToPeer(
		context.Background(),
		transport,
		"node-B",
	)
	if err != nil {
		t.Fatal(err)
	}

	if ok {
		t.Error("expected replication not to succeed")
	}

	if leader.State() != StateFollower {
		t.Errorf("leader state = %v, want follower", leader.State())
	}

	if leader.CurrentTerm() != 2 {
		t.Errorf("leader term = %d, want 2", leader.CurrentTerm())
	}
}

func TestReplicateToPeerRejectsNonLeader(t *testing.T) {
	leader, _, transport := newReplicationTestCluster(
		t,
		nil,
		nil,
	)

	leader.mu.Lock()
	leader.state = StateFollower
	leader.mu.Unlock()

	ok, err := leader.ReplicateToPeer(
		context.Background(),
		transport,
		"node-B",
	)
	if err != nil {
		t.Fatal(err)
	}

	if ok {
		t.Error("expected follower to reject replication")
	}
}

func TestReplicateToPeerUntilCaughtUp(t *testing.T) {
	leaderEntries := []LogEntry{
		{Term: 1, Command: []byte("task-1")},
		{Term: 1, Command: []byte("task-2")},
		{Term: 1, Command: []byte("task-3")},
	}

	followerEntries := []LogEntry{
		{Term: 1, Command: []byte("task-1")},
	}

	leader, follower, transport := newReplicationTestCluster(
		t,
		leaderEntries,
		followerEntries,
	)

	ok, err := leader.ReplicateToPeerUntilCaughtUp(
		context.Background(),
		transport,
		"node-B",
	)
	if err != nil {
		t.Fatal(err)
	}

	if !ok {
		t.Fatal("expected follower to catch up")
	}

	follower.mu.RLock()
	defer follower.mu.RUnlock()

	if len(follower.log) != 3 {
		t.Fatalf(
			"expected follower to have 3 entries, got %d",
			len(follower.log),
		)
	}

	for i, expected := range leaderEntries {
		if string(follower.log[i].Command) != string(expected.Command) {
			t.Errorf(
				"entry %d: expected %q, got %q",
				i,
				expected.Command,
				follower.log[i].Command,
			)
		}
	}
}
