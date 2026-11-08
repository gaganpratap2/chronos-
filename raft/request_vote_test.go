package raft

import (
	"sync"
	"testing"
)

func newRequestVoteTestNode(t *testing.T) (*Node, *MemoryStorage) {
	t.Helper()

	storage := NewMemoryStorage()

	node, err := NewNodeWithStorage(
		"node-A",
		[]string{"node-B", "node-C"},
		storage,
	)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	return node, storage
}

func TestRequestVoteGrantsVote(t *testing.T) {
	node, storage := newRequestVoteTestNode(t)

	response, err := node.RequestVote(RequestVoteRequest{
		Term:         1,
		CandidateID:  "node-B",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if err != nil {
		t.Fatalf("RequestVote returned error: %v", err)
	}

	if !response.VoteGranted {
		t.Fatal("expected vote to be granted")
	}

	if response.Term != 1 {
		t.Fatalf("expected term 1, got %d", response.Term)
	}

	if node.VotedFor() != "node-B" {
		t.Fatalf("expected vote for node-B, got %q", node.VotedFor())
	}

	saved, err := storage.LoadHardState()
	if err != nil {
		t.Fatalf("load hard state: %v", err)
	}

	if saved.CurrentTerm != 1 || saved.VotedFor != "node-B" {
		t.Fatalf("unexpected persisted hard state: %+v", saved)
	}
}

func TestRequestVoteRejectsOlderTerm(t *testing.T) {
	storage := NewMemoryStorage()

	if err := storage.SaveHardState(HardState{
		CurrentTerm: 2,
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

	response, err := node.RequestVote(RequestVoteRequest{
		Term:        1,
		CandidateID: "node-B",
	})
	if err != nil {
		t.Fatal(err)
	}

	if response.VoteGranted {
		t.Fatal("must reject an older term")
	}

	if response.Term != 2 {
		t.Fatalf("expected current term 2, got %d", response.Term)
	}
}

func TestRequestVoteRejectsSecondCandidateInSameTerm(t *testing.T) {
	node, _ := newRequestVoteTestNode(t)

	first, err := node.RequestVote(RequestVoteRequest{
		Term:        1,
		CandidateID: "node-B",
	})
	if err != nil {
		t.Fatal(err)
	}

	if !first.VoteGranted {
		t.Fatal("expected first candidate to receive vote")
	}

	second, err := node.RequestVote(RequestVoteRequest{
		Term:        1,
		CandidateID: "node-C",
	})
	if err != nil {
		t.Fatal(err)
	}

	if second.VoteGranted {
		t.Fatal("must not grant a second vote in the same term")
	}

	if node.VotedFor() != "node-B" {
		t.Fatalf("vote changed unexpectedly to %q", node.VotedFor())
	}
}

func TestRequestVoteRejectsOutdatedLog(t *testing.T) {
	node, _ := newRequestVoteTestNode(t)

	// The receiver has a more recent log.
	node.lastLogIndex = 8
	node.lastLogTerm = 3

	response, err := node.RequestVote(RequestVoteRequest{
		Term:         4,
		CandidateID:  "node-B",
		LastLogIndex: 100,
		LastLogTerm:  2,
	})
	if err != nil {
		t.Fatal(err)
	}

	if response.VoteGranted {
		t.Fatal("must reject a candidate with an older last log term")
	}

	if node.VotedFor() != "" {
		t.Fatalf("expected no vote, got %q", node.VotedFor())
	}
}

func TestRequestVotePersistsHigherTermEvenWhenLogIsOutdated(t *testing.T) {
	node, storage := newRequestVoteTestNode(t)

	node.lastLogIndex = 8
	node.lastLogTerm = 3

	response, err := node.RequestVote(RequestVoteRequest{
		Term:         5,
		CandidateID:  "node-B",
		LastLogIndex: 100,
		LastLogTerm:  2,
	})
	if err != nil {
		t.Fatal(err)
	}

	if response.VoteGranted {
		t.Fatal("expected outdated candidate to be rejected")
	}

	if node.CurrentTerm() != 5 {
		t.Fatalf("expected current term 5, got %d", node.CurrentTerm())
	}

	saved, err := storage.LoadHardState()
	if err != nil {
		t.Fatal(err)
	}

	if saved.CurrentTerm != 5 || saved.VotedFor != "" {
		t.Fatalf("higher term was not persisted correctly: %+v", saved)
	}
}

func TestRequestVoteRejectsUnknownCandidate(t *testing.T) {
	node, _ := newRequestVoteTestNode(t)

	response, err := node.RequestVote(RequestVoteRequest{
		Term:        1,
		CandidateID: "unknown-node",
	})
	if err != nil {
		t.Fatal(err)
	}

	if response.VoteGranted {
		t.Fatal("must reject an unknown candidate")
	}
}

func TestRequestVoteRejectsInvalidRequest(t *testing.T) {
	tests := []struct {
		name string
		req  RequestVoteRequest
	}{
		{
			name: "zero term",
			req: RequestVoteRequest{
				CandidateID: "node-B",
			},
		},
		{
			name: "empty candidate",
			req: RequestVoteRequest{
				Term: 1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node, _ := newRequestVoteTestNode(t)

			response, err := node.RequestVote(tt.req)
			if err != nil {
				t.Fatal(err)
			}

			if response.VoteGranted {
				t.Fatal("invalid request must not receive a vote")
			}
		})
	}
}

func TestRequestVoteDoesNotGrantVoteWhenStorageFails(t *testing.T) {
	storage := &failingStorage{
		state: HardState{
			CurrentTerm: 1,
		},
		failSave: true,
	}

	node, err := NewNodeWithStorage(
		"node-A",
		[]string{"node-B"},
		storage,
	)
	if err != nil {
		t.Fatal(err)
	}

	response, err := node.RequestVote(RequestVoteRequest{
		Term:        1,
		CandidateID: "node-B",
	})

	if err != errStorageUnavailable {
		t.Fatalf("expected storage error, got %v", err)
	}

	if response.VoteGranted {
		t.Fatal("must not grant a vote when persistence fails")
	}

	if node.VotedFor() != "" {
		t.Fatalf("vote changed despite storage failure: %q", node.VotedFor())
	}
}

func TestRequestVoteDoesNotChangeTermWhenHigherTermPersistenceFails(t *testing.T) {
	storage := &failingStorage{
		state: HardState{
			CurrentTerm: 4,
			VotedFor:    "node-B",
		},
		failSave: true,
	}

	node, err := NewNodeWithStorage(
		"node-A",
		[]string{"node-B", "node-C"},
		storage,
	)
	if err != nil {
		t.Fatal(err)
	}

	_, err = node.RequestVote(RequestVoteRequest{
		Term:        5,
		CandidateID: "node-C",
	})

	if err != errStorageUnavailable {
		t.Fatalf("expected storage error, got %v", err)
	}

	if node.CurrentTerm() != 4 {
		t.Fatalf("expected term 4, got %d", node.CurrentTerm())
	}

	if node.VotedFor() != "node-B" {
		t.Fatalf("expected existing vote for node-B, got %q", node.VotedFor())
	}
}

func TestRequestVoteAllowsOnlyOneConcurrentCandidate(t *testing.T) {
	node, _ := newRequestVoteTestNode(t)

	candidates := []string{"node-B", "node-C"}
	results := make(chan bool, len(candidates))

	var wg sync.WaitGroup

	for _, candidateID := range candidates {
		wg.Add(1)

		go func(candidateID string) {
			defer wg.Done()

			response, err := node.RequestVote(RequestVoteRequest{
				Term:        1,
				CandidateID: candidateID,
			})
			if err != nil {
				t.Errorf("RequestVote for %s: %v", candidateID, err)
				return
			}

			results <- response.VoteGranted
		}(candidateID)
	}

	wg.Wait()
	close(results)

	granted := 0

	for voteGranted := range results {
		if voteGranted {
			granted++
		}
	}

	if granted != 1 {
		t.Fatalf("expected exactly one granted vote, got %d", granted)
	}

	if node.VotedFor() == "" {
		t.Fatal("expected node to remember its granted vote")
	}
}

func TestRequestVoteAcceptsEqualTermAndIndexLog(t *testing.T) {
	node, _ := newRequestVoteTestNode(t)

	node.lastLogIndex = 8
	node.lastLogTerm = 3

	response, err := node.RequestVote(RequestVoteRequest{
		Term:         4,
		CandidateID:  "node-B",
		LastLogIndex: 8,
		LastLogTerm:  3,
	})
	if err != nil {
		t.Fatal(err)
	}

	if !response.VoteGranted {
		t.Fatal("expected candidate with equally up-to-date log to receive vote")
	}
}

func TestRequestVoteHigherLogTermWins(t *testing.T) {
	node, _ := newRequestVoteTestNode(t)

	node.lastLogIndex = 10
	node.lastLogTerm = 3

	response, err := node.RequestVote(RequestVoteRequest{
		Term:         4,
		CandidateID:  "node-B",
		LastLogIndex: 5,
		LastLogTerm:  4,
	})
	if err != nil {
		t.Fatal(err)
	}

	if !response.VoteGranted {
		t.Fatal("expected candidate with higher last log term to receive vote")
	}
}

func TestRequestVoteHigherTermMakesCandidateFollower(t *testing.T) {
	node, _ := newRequestVoteTestNode(t)

	if err := node.StartElection(); err != nil {
		t.Fatal(err)
	}

	if node.State() != StateCandidate {
		t.Fatal("expected candidate state after starting election")
	}

	response, err := node.RequestVote(RequestVoteRequest{
		Term:        2,
		CandidateID: "node-B",
	})
	if err != nil {
		t.Fatal(err)
	}

	if !response.VoteGranted {
		t.Fatal("expected vote to be granted")
	}

	if node.State() != StateFollower {
		t.Fatalf("expected follower state, got %s", node.State())
	}

	if node.CurrentTerm() != 2 {
		t.Fatalf("expected term 2, got %d", node.CurrentTerm())
	}

	if node.VotedFor() != "node-B" {
		t.Fatalf("expected vote for node-B, got %q", node.VotedFor())
	}
}
