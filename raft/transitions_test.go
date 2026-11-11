package raft

import (
	"context"
	"errors"
	"testing"
)

func TestStartElection(t *testing.T) {
	node, err := NewNode("node-A", []string{"node-B", "node-C"})
	if err != nil {
		t.Fatal(err)
	}

	if err := node.StartElection(); err != nil {
		t.Fatal(err)
	}

	if node.State() != StateCandidate {
		t.Errorf("State() = %v, want candidate", node.State())
	}

	if node.CurrentTerm() != 1 {
		t.Errorf("CurrentTerm() = %d, want 1", node.CurrentTerm())
	}

	if node.VotedFor() != "node-A" {
		t.Errorf("VotedFor() = %q, want node-A", node.VotedFor())
	}
}

func TestStartElectionIncrementsTerm(t *testing.T) {
	node, err := NewNode("node-A", nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := node.StartElection(); err != nil {
		t.Fatal(err)
	}

	if err := node.StartElection(); err != nil {
		t.Fatal(err)
	}

	if node.CurrentTerm() != 2 {
		t.Errorf("CurrentTerm() = %d, want 2", node.CurrentTerm())
	}
}

func TestBecomeLeader(t *testing.T) {
	node, err := NewNode("node-A", nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := node.StartElection(); err != nil {
		t.Fatal(err)
	}

	if err := node.becomeLeaderIfMajority(1, 2); err != nil {
		t.Fatal(err)
	}

	if node.State() != StateLeader {
		t.Errorf("State() = %v, want leader", node.State())
	}
}

func TestBecomeLeaderRejectsFollower(t *testing.T) {
	node, err := NewNode("node-A", []string{"node-B", "node-C"})
	if err != nil {
		t.Fatal(err)
	}

	err = node.becomeLeaderIfMajority(0, 3)

	if !errors.Is(err, ErrNotCandidate) {
		t.Errorf("error = %v, want ErrNotCandidate", err)
	}
}

func TestObserveHigherTerm(t *testing.T) {
	node, err := NewNode("node-A", nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := node.StartElection(); err != nil {
		t.Fatal(err)
	}

	if err := node.becomeLeaderIfMajority(1, 2); err != nil {
		t.Fatal(err)
	}

	if err := node.ObserveHigherTerm(5); err != nil {
		t.Fatal(err)
	}

	if node.CurrentTerm() != 5 {
		t.Errorf("CurrentTerm() = %d, want 5", node.CurrentTerm())
	}

	if node.State() != StateFollower {
		t.Errorf("State() = %v, want follower", node.State())
	}

	if node.VotedFor() != "" {
		t.Errorf("VotedFor() = %q, want empty", node.VotedFor())
	}
}

func TestObserveHigherTermRejectsOldTerm(t *testing.T) {
	node, err := NewNode("node-A", nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := node.StartElection(); err != nil {
		t.Fatal(err)
	}

	err = node.ObserveHigherTerm(1)

	if !errors.Is(err, ErrTermNotHigher) {
		t.Errorf("error = %v, want ErrTermNotHigher", err)
	}
}

func TestBecomeLeaderWithMajority(t *testing.T) {
	node, err := NewNode("node-A", []string{"node-B", "node-C"})
	if err != nil {
		t.Fatal(err)
	}

	if err := node.StartElection(); err != nil {
		t.Fatal(err)
	}

	err = node.becomeLeaderIfMajority(1, 2)
	if err != nil {
		t.Fatalf("expected successful leadership, got %v", err)
	}

	if node.State() != StateLeader {
		t.Errorf("State() = %v, want leader", node.State())
	}
}

func TestBecomeLeaderRejectsInsufficientVotes(t *testing.T) {
	node, err := NewNode("node-A", []string{"node-B", "node-C"})
	if err != nil {
		t.Fatal(err)
	}

	if err := node.StartElection(); err != nil {
		t.Fatal(err)
	}

	err = node.becomeLeaderIfMajority(1, 1)

	if !errors.Is(err, ErrInsufficientVotes) {
		t.Fatalf("error = %v, want ErrInsufficientVotes", err)
	}

	if node.State() != StateCandidate {
		t.Errorf("State() = %v, want candidate", node.State())
	}
}

func TestBecomeLeaderInitializesReplicationProgress(t *testing.T) {
	node, err := NewNode("node-A", []string{"node-B", "node-C"})
	if err != nil {
		t.Fatal(err)
	}

	// Simulate a candidate with three existing log entries.
	node.log = []LogEntry{
		{Term: 1, Command: []byte("task-1")},
		{Term: 1, Command: []byte("task-2")},
		{Term: 2, Command: []byte("task-3")},
	}
	node.lastLogIndex = 3
	node.lastLogTerm = 2

	if err := node.StartElection(); err != nil {
		t.Fatal(err)
	}

	if err := node.becomeLeaderIfMajority(1, 2); err != nil {
		t.Fatal(err)
	}

	// Every follower must start replication after the leader's last entry.
	for _, peerID := range []string{"node-B", "node-C"} {
		if got := node.nextIndex[peerID]; got != 4 {
			t.Errorf("nextIndex[%q] = %d, want 4", peerID, got)
		}

		if got := node.matchIndex[peerID]; got != 0 {
			t.Errorf("matchIndex[%q] = %d, want 0", peerID, got)
		}
	}

	// The leader already has its own log.
	if got := node.matchIndex["node-A"]; got != 3 {
		t.Errorf("matchIndex[node-A] = %d, want 3", got)
	}
}

func TestBecomeLeaderInitializesReplicationProgressWithEmptyLog(t *testing.T) {
	node, err := NewNode("node-A", []string{"node-B"})
	if err != nil {
		t.Fatal(err)
	}

	if err := node.StartElection(); err != nil {
		t.Fatal(err)
	}

	if err := node.becomeLeaderIfMajority(1, 2); err != nil {
		t.Fatal(err)
	}

	if got := node.nextIndex["node-B"]; got != 1 {
		t.Errorf("nextIndex[node-B] = %d, want 1", got)
	}

	if got := node.matchIndex["node-B"]; got != 0 {
		t.Errorf("matchIndex[node-B] = %d, want 0", got)
	}

	if got := node.matchIndex["node-A"]; got != 0 {
		t.Errorf("matchIndex[node-A] = %d, want 0", got)
	}
}

func TestRecordReplicationSuccess(t *testing.T) {
	node, err := NewNode("node-A", []string{"node-B"})
	if err != nil {
		t.Fatal(err)
	}

	node.log = []LogEntry{
		{Term: 1, Command: []byte("task-1")},
		{Term: 1, Command: []byte("task-2")},
	}

	if err := node.StartElection(); err != nil {
		t.Fatal(err)
	}

	if err := node.becomeLeaderIfMajority(1, 2); err != nil {
		t.Fatal(err)
	}

	node.mu.Lock()
	ok := node.recordReplicationSuccessLocked("node-B", 1, 2)
	node.mu.Unlock()

	if !ok {
		t.Fatal("expected replication success to be recorded")
	}

	if got := node.matchIndex["node-B"]; got != 2 {
		t.Errorf("matchIndex[node-B] = %d, want 2", got)
	}

	if got := node.nextIndex["node-B"]; got != 3 {
		t.Errorf("nextIndex[node-B] = %d, want 3", got)
	}
}

func TestRecordReplicationSuccessNeverRegressesProgress(t *testing.T) {
	node, err := NewNode("node-A", []string{"node-B"})
	if err != nil {
		t.Fatal(err)
	}

	node.log = []LogEntry{
		{Term: 1},
		{Term: 1},
		{Term: 1},
	}

	if err := node.StartElection(); err != nil {
		t.Fatal(err)
	}

	if err := node.becomeLeaderIfMajority(1, 2); err != nil {
		t.Fatal(err)
	}

	node.mu.Lock()
	node.recordReplicationSuccessLocked("node-B", 1, 3)
	node.recordReplicationSuccessLocked("node-B", 1, 1)
	node.mu.Unlock()

	if got := node.matchIndex["node-B"]; got != 3 {
		t.Errorf("matchIndex[node-B] = %d, want 3", got)
	}

	if got := node.nextIndex["node-B"]; got != 4 {
		t.Errorf("nextIndex[node-B] = %d, want 4", got)
	}
}

func TestRecordReplicationFailure(t *testing.T) {
	node, err := NewNode("node-A", []string{"node-B"})
	if err != nil {
		t.Fatal(err)
	}

	node.log = []LogEntry{
		{Term: 1},
		{Term: 1},
		{Term: 1},
	}

	if err := node.StartElection(); err != nil {
		t.Fatal(err)
	}

	if err := node.becomeLeaderIfMajority(1, 2); err != nil {
		t.Fatal(err)
	}

	node.mu.Lock()
	ok := node.recordReplicationFailureLocked("node-B", 1, 4)
	node.mu.Unlock()

	if !ok {
		t.Fatal("expected replication failure to be recorded")
	}

	if got := node.nextIndex["node-B"]; got != 3 {
		t.Errorf("nextIndex[node-B] = %d, want 3", got)
	}

	if got := node.matchIndex["node-B"]; got != 0 {
		t.Errorf("matchIndex[node-B] = %d, want 0", got)
	}
}

func TestRecordReplicationFailureDoesNotPassConfirmedProgress(t *testing.T) {
	node, err := NewNode("node-A", []string{"node-B"})
	if err != nil {
		t.Fatal(err)
	}

	node.log = []LogEntry{
		{Term: 1},
		{Term: 1},
		{Term: 1},
	}

	if err := node.StartElection(); err != nil {
		t.Fatal(err)
	}

	if err := node.becomeLeaderIfMajority(1, 2); err != nil {
		t.Fatal(err)
	}

	node.mu.Lock()
	node.recordReplicationSuccessLocked("node-B", 1, 2)
	ok := node.recordReplicationFailureLocked("node-B", 1, 3)
	node.mu.Unlock()

	if !ok {
		t.Fatal("expected failure to be handled")
	}

	if got := node.nextIndex["node-B"]; got != 3 {
		t.Errorf("nextIndex[node-B] = %d, want 3", got)
	}
}

func TestRecordReplicationRejectsStaleAndUnknownResponses(t *testing.T) {
	node, err := NewNode("node-A", []string{"node-B"})
	if err != nil {
		t.Fatal(err)
	}

	if err := node.StartElection(); err != nil {
		t.Fatal(err)
	}

	if err := node.becomeLeaderIfMajority(1, 2); err != nil {
		t.Fatal(err)
	}

	node.mu.Lock()

	staleTerm := node.recordReplicationSuccessLocked("node-B", 0, 0)
	unknownPeer := node.recordReplicationSuccessLocked("node-X", 1, 0)
	invalidIndex := node.recordReplicationSuccessLocked("node-B", 1, 1)

	node.mu.Unlock()

	if staleTerm {
		t.Error("expected stale-term response to be rejected")
	}

	if unknownPeer {
		t.Error("expected unknown peer response to be rejected")
	}

	if invalidIndex {
		t.Error("expected index beyond local log to be rejected")
	}
}

func TestRecordReplicationFailureRejectsStaleRequest(t *testing.T) {
	node, err := NewNode("node-A", []string{"node-B"})
	if err != nil {
		t.Fatal(err)
	}

	if err := node.StartElection(); err != nil {
		t.Fatal(err)
	}

	if err := node.becomeLeaderIfMajority(1, 2); err != nil {
		t.Fatal(err)
	}

	node.mu.Lock()
	ok := node.recordReplicationFailureLocked("node-B", 1, 99)
	node.mu.Unlock()

	if ok {
		t.Error("expected stale replication request to be rejected")
	}

	if got := node.nextIndex["node-B"]; got != 1 {
		t.Errorf("nextIndex[node-B] = %d, want 1", got)
	}
}

func TestAdvanceCommitIndexWithMajority(t *testing.T) {
	node, err := NewNode("node-A", []string{"node-B", "node-C"})
	if err != nil {
		t.Fatal(err)
	}

	node.log = []LogEntry{
		{Term: 1, Command: []byte("task-1")},
		{Term: 1, Command: []byte("task-2")},
	}
	node.lastLogIndex = 2
	node.lastLogTerm = 1

	if err := node.StartElection(); err != nil {
		t.Fatal(err)
	}

	if err := node.becomeLeaderIfMajority(1, 2); err != nil {
		t.Fatal(err)
	}

	node.mu.Lock()
	node.matchIndex["node-B"] = 2
	advanced := node.advanceCommitIndexLocked()
	commitIndex := node.commitIndex
	node.mu.Unlock()

	if !advanced {
		t.Fatal("expected commit index to advance")
	}

	if commitIndex != 2 {
		t.Errorf("commitIndex = %d, want 2", commitIndex)
	}
}

func TestAdvanceCommitIndexRejectsInsufficientReplication(t *testing.T) {
	node, err := NewNode("node-A", []string{"node-B", "node-C"})
	if err != nil {
		t.Fatal(err)
	}

	node.log = []LogEntry{
		{Term: 1, Command: []byte("task-1")},
	}

	if err := node.StartElection(); err != nil {
		t.Fatal(err)
	}

	if err := node.becomeLeaderIfMajority(1, 2); err != nil {
		t.Fatal(err)
	}

	node.mu.Lock()
	advanced := node.advanceCommitIndexLocked()
	commitIndex := node.commitIndex
	node.mu.Unlock()

	if advanced {
		t.Error("unexpected commit advancement without a majority")
	}

	if commitIndex != 0 {
		t.Errorf("commitIndex = %d, want 0", commitIndex)
	}
}

func TestAdvanceCommitIndexRequiresCurrentTermEntry(t *testing.T) {
	storage := NewMemoryStorage()

	// Simulate a node that has already persisted term 1.
	if err := storage.SaveHardState(HardState{
		CurrentTerm: 1,
	}); err != nil {
		t.Fatal(err)
	}

	node, err := NewNodeWithStorage(
		"node-A",
		[]string{"node-B"},
		storage,
	)
	if err != nil {
		t.Fatal(err)
	}

	// This entry belongs to the previous term.
	node.log = []LogEntry{
		{Term: 1, Command: []byte("old-term-task")},
	}
	node.lastLogIndex = 1
	node.lastLogTerm = 1

	// Election advances the node to term 2.
	if err := node.StartElection(); err != nil {
		t.Fatal(err)
	}

	if node.CurrentTerm() != 2 {
		t.Fatalf("CurrentTerm() = %d, want 2", node.CurrentTerm())
	}

	if err := node.becomeLeaderIfMajority(2, 2); err != nil {
		t.Fatal(err)
	}

	node.mu.Lock()
	node.matchIndex["node-B"] = 1
	advanced := node.advanceCommitIndexLocked()
	commitIndex := node.commitIndex
	node.mu.Unlock()

	if advanced {
		t.Error("expected old-term entry not to be committed directly")
	}

	if commitIndex != 0 {
		t.Errorf("commitIndex = %d, want 0", commitIndex)
	}
}

func TestAdvanceCommitIndexSingleNodeCluster(t *testing.T) {
	node, err := NewNode("node-A", nil)
	if err != nil {
		t.Fatal(err)
	}

	node.log = []LogEntry{
		{Term: 1, Command: []byte("task-1")},
	}

	if err := node.StartElection(); err != nil {
		t.Fatal(err)
	}

	if err := node.becomeLeaderIfMajority(1, 1); err != nil {
		t.Fatal(err)
	}

	node.mu.Lock()
	advanced := node.advanceCommitIndexLocked()
	commitIndex := node.commitIndex
	node.mu.Unlock()

	if !advanced {
		t.Fatal("single-node leader should commit its own current-term entry")
	}

	if commitIndex != 1 {
		t.Errorf("commitIndex = %d, want 1", commitIndex)
	}
}

func TestReplicateToAllPeersReplicatesConcurrently(t *testing.T) {
	leader, err := NewNode(
		"node-A",
		[]string{"node-B", "node-C"},
	)
	if err != nil {
		t.Fatal(err)
	}

	leaderEntries := []LogEntry{
		{Term: 1, Command: []byte("task-1")},
		{Term: 1, Command: []byte("task-2")},
	}

	leader.log = leaderEntries
	leader.lastLogIndex = uint64(len(leaderEntries))
	leader.lastLogTerm = 1

	if err := leader.StartElection(); err != nil {
		t.Fatal(err)
	}

	if err := leader.becomeLeaderIfMajority(1, 3); err != nil {
		t.Fatal(err)
	}

	followerB, err := NewNode("node-B", []string{"node-A"})
	if err != nil {
		t.Fatal(err)
	}

	followerB.log = []LogEntry{
		{Term: 1, Command: []byte("task-1")},
	}
	followerB.lastLogIndex = 1
	followerB.lastLogTerm = 1

	followerC, err := NewNode("node-C", []string{"node-A"})
	if err != nil {
		t.Fatal(err)
	}

	transport := NewInMemoryAppendEntriesTransport()

	if err := transport.Register("node-B", followerB); err != nil {
		t.Fatal(err)
	}

	if err := transport.Register("node-C", followerC); err != nil {
		t.Fatal(err)
	}

	results, err := leader.ReplicateToAllPeers(
		context.Background(),
		transport,
	)
	if err != nil {
		t.Fatal(err)
	}

	if !results["node-B"] {
		t.Error("expected node-B replication to succeed")
	}

	if !results["node-C"] {
		t.Error("expected node-C replication to succeed")
	}

	for _, follower := range []*Node{followerB, followerC} {
		follower.mu.RLock()

		if len(follower.log) != 2 {
			t.Errorf(
				"%s: expected 2 log entries, got %d",
				follower.id,
				len(follower.log),
			)
		}

		follower.mu.RUnlock()
	}
}

func TestAdvanceCommitIndexRequiresBothJointMajorities(t *testing.T) {
	tests := []struct {
		name          string
		replicated    string
		wantCommitted bool
	}{
		{
			name:          "old majority only",
			replicated:    "node-C",
			wantCommitted: false,
		},
		{
			name:          "new majority only",
			replicated:    "node-D",
			wantCommitted: false,
		},
		{
			name:          "both majorities",
			replicated:    "node-B",
			wantCommitted: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node, err := NewNode(
				"node-A",
				[]string{"node-B", "node-C"},
			)
			if err != nil {
				t.Fatal(err)
			}

			node.configuration = ConfigurationState{
				OldVoters: []string{"node-A", "node-B", "node-C"},
				NewVoters: []string{"node-A", "node-B", "node-D"},
			}

			node.log = []LogEntry{
				{Term: 1, Command: []byte("task-1")},
			}
			node.lastLogIndex = 1
			node.lastLogTerm = 1

			if err := node.StartElection(); err != nil {
				t.Fatal(err)
			}

			if err := node.becomeLeaderIfMajority(1, 2); err != nil {
				t.Fatal(err)
			}

			node.mu.Lock()
			node.matchIndex[tt.replicated] = 1

			advanced := node.advanceCommitIndexLocked()
			commitIndex := node.commitIndex
			node.mu.Unlock()

			if advanced != tt.wantCommitted {
				t.Errorf(
					"advanceCommitIndexLocked() = %v, want %v",
					advanced,
					tt.wantCommitted,
				)
			}

			wantIndex := uint64(0)
			if tt.wantCommitted {
				wantIndex = 1
			}

			if commitIndex != wantIndex {
				t.Errorf(
					"commitIndex = %d, want %d",
					commitIndex,
					wantIndex,
				)
			}
		})
	}
}
