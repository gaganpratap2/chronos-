package raft

import (
	"context"
	"testing"
)

type blockingVoteTransport struct {
	started chan struct{}
}

func (t *blockingVoteTransport) RequestVote(
	ctx context.Context,
	peerID string,
	req RequestVoteRequest,
) (RequestVoteResponse, error) {
	select {
	case t.started <- struct{}{}:
	default:
	}

	<-ctx.Done()

	return RequestVoteResponse{}, ctx.Err()
}

func TestElectionCoordinatorWinsThreeNodeElection(t *testing.T) {
	nodeA, err := NewNode(
		"node-A",
		[]string{"node-B", "node-C"},
	)
	if err != nil {
		t.Fatal(err)
	}

	nodeB, err := NewNode(
		"node-B",
		[]string{"node-A", "node-C"},
	)
	if err != nil {
		t.Fatal(err)
	}

	nodeC, err := NewNode(
		"node-C",
		[]string{"node-A", "node-B"},
	)
	if err != nil {
		t.Fatal(err)
	}

	transport := NewInMemoryVoteTransport()

	if err := transport.Register("node-B", nodeB); err != nil {
		t.Fatal(err)
	}

	if err := transport.Register("node-C", nodeC); err != nil {
		t.Fatal(err)
	}

	coordinator, err := NewElectionCoordinator(nodeA, transport)
	if err != nil {
		t.Fatal(err)
	}

	elected, err := coordinator.RunElection(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if !elected {
		t.Fatal("expected node-A to win the election")
	}

	if nodeA.State() != StateLeader {
		t.Fatalf("node-A state = %v, want leader", nodeA.State())
	}

	if nodeA.CurrentTerm() != 1 {
		t.Errorf("node-A term = %d, want 1", nodeA.CurrentTerm())
	}

	votesFromPeers := 0

	if nodeB.VotedFor() == "node-A" {
		votesFromPeers++
	}

	if nodeC.VotedFor() == "node-A" {
		votesFromPeers++
	}

	if votesFromPeers < 1 {
		t.Fatal("expected at least one peer to vote for node-A")
	}
}

func TestElectionCoordinatorWinsFiveNodeElection(t *testing.T) {
	nodeA, err := NewNode(
		"node-A",
		[]string{"node-B", "node-C", "node-D", "node-E"},
	)
	if err != nil {
		t.Fatal(err)
	}

	nodeB, err := NewNode("node-B", []string{"node-A"})
	if err != nil {
		t.Fatal(err)
	}

	nodeC, err := NewNode("node-C", []string{"node-A"})
	if err != nil {
		t.Fatal(err)
	}

	transport := NewInMemoryVoteTransport()

	if err := transport.Register("node-B", nodeB); err != nil {
		t.Fatal(err)
	}

	if err := transport.Register("node-C", nodeC); err != nil {
		t.Fatal(err)
	}

	coordinator, err := NewElectionCoordinator(nodeA, transport)
	if err != nil {
		t.Fatal(err)
	}

	elected, err := coordinator.RunElection(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if !elected {
		t.Fatal("expected node-A to win with three votes")
	}

	if nodeA.State() != StateLeader {
		t.Fatalf("node-A state = %v, want leader", nodeA.State())
	}
}

func TestElectionCoordinatorCannotWinWithoutMajority(t *testing.T) {
	nodeA, err := NewNode(
		"node-A",
		[]string{"node-B", "node-C", "node-D", "node-E"},
	)
	if err != nil {
		t.Fatal(err)
	}

	nodeB, err := NewNode("node-B", []string{"node-A"})
	if err != nil {
		t.Fatal(err)
	}

	transport := NewInMemoryVoteTransport()

	if err := transport.Register("node-B", nodeB); err != nil {
		t.Fatal(err)
	}

	coordinator, err := NewElectionCoordinator(nodeA, transport)
	if err != nil {
		t.Fatal(err)
	}

	elected, err := coordinator.RunElection(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if elected {
		t.Fatal("node-A must not win without a majority")
	}

	if nodeA.State() != StateCandidate {
		t.Fatalf("node-A state = %v, want candidate", nodeA.State())
	}
}

func TestElectionCoordinatorSingleNodeCluster(t *testing.T) {
	node, err := NewNode("node-A", nil)
	if err != nil {
		t.Fatal(err)
	}

	transport := NewInMemoryVoteTransport()

	coordinator, err := NewElectionCoordinator(node, transport)
	if err != nil {
		t.Fatal(err)
	}

	elected, err := coordinator.RunElection(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if !elected {
		t.Fatal("single-node cluster should elect itself")
	}

	if node.State() != StateLeader {
		t.Fatalf("state = %v, want leader", node.State())
	}
}

func TestElectionCoordinatorStepsDownOnHigherTerm(t *testing.T) {
	nodeA, err := NewNode("node-A", []string{"node-B", "node-C"})
	if err != nil {
		t.Fatal(err)
	}

	nodeB, err := NewNode("node-B", []string{"node-A"})
	if err != nil {
		t.Fatal(err)
	}

	if err := nodeB.ObserveHigherTerm(5); err != nil {
		t.Fatal(err)
	}

	transport := NewInMemoryVoteTransport()

	if err := transport.Register("node-B", nodeB); err != nil {
		t.Fatal(err)
	}

	coordinator, err := NewElectionCoordinator(nodeA, transport)
	if err != nil {
		t.Fatal(err)
	}

	elected, err := coordinator.RunElection(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if elected {
		t.Fatal("node-A must not win after discovering a higher term")
	}

	if nodeA.State() != StateFollower {
		t.Fatalf("node-A state = %v, want follower", nodeA.State())
	}

	if nodeA.CurrentTerm() != 5 {
		t.Errorf("node-A term = %d, want 5", nodeA.CurrentTerm())
	}
}

func TestElectionCoordinatorStopsOnContextCancellation(t *testing.T) {
	nodeA, err := NewNode(
		"node-A",
		[]string{"node-B", "node-C"},
	)
	if err != nil {
		t.Fatal(err)
	}

	transport := &blockingVoteTransport{
		started: make(chan struct{}, 1),
	}

	coordinator, err := NewElectionCoordinator(nodeA, transport)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	result := make(chan error, 1)

	go func() {
		_, err := coordinator.RunElection(ctx)
		result <- err
	}()

	// Wait until a peer request has started.
	<-transport.started

	cancel()

	err = <-result

	if err != context.Canceled {
		t.Fatalf("error = %v, want context.Canceled", err)
	}

	if nodeA.State() != StateCandidate {
		t.Fatalf(
			"node-A state = %v, want candidate",
			nodeA.State(),
		)
	}

	if nodeA.State() == StateLeader {
		t.Fatal("cancelled election must not make node-A leader")
	}
}

func TestNewElectionCoordinatorRejectsNilNode(t *testing.T) {
	transport := NewInMemoryVoteTransport()

	_, err := NewElectionCoordinator(nil, transport)

	if err != ErrNilElectionNode {
		t.Fatalf(
			"error = %v, want %v",
			err,
			ErrNilElectionNode,
		)
	}
}

func TestNewElectionCoordinatorRejectsNilTransport(t *testing.T) {
	node, err := NewNode("node-A", nil)
	if err != nil {
		t.Fatal(err)
	}

	_, err = NewElectionCoordinator(node, nil)

	if err != ErrNilVoteTransport {
		t.Fatalf(
			"error = %v, want %v",
			err,
			ErrNilVoteTransport,
		)
	}
}
