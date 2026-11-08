package raft

import (
	"context"
	"errors"
	"testing"
)

func TestInMemoryVoteTransportDeliversRequest(t *testing.T) {
	nodeA, err := NewNode("node-A", []string{"node-B"})
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

	if err := nodeA.StartElection(); err != nil {
		t.Fatal(err)
	}

	response, err := transport.RequestVote(
		context.Background(),
		"node-B",
		RequestVoteRequest{
			Term:        1,
			CandidateID: "node-A",
		},
	)
	if err != nil {
		t.Fatalf("RequestVote returned error: %v", err)
	}

	if !response.VoteGranted {
		t.Fatal("expected node-B to grant its vote")
	}

	if nodeB.VotedFor() != "node-A" {
		t.Fatalf("node-B voted for %q, want node-A", nodeB.VotedFor())
	}
}

func TestInMemoryVoteTransportRejectsUnknownPeer(t *testing.T) {
	transport := NewInMemoryVoteTransport()

	_, err := transport.RequestVote(
		context.Background(),
		"unknown-node",
		RequestVoteRequest{},
	)

	if !errors.Is(err, ErrPeerUnavailable) {
		t.Fatalf("error = %v, want ErrPeerUnavailable", err)
	}
}

func TestInMemoryVoteTransportRejectsCanceledContext(t *testing.T) {
	nodeB, err := NewNode("node-B", []string{"node-A"})
	if err != nil {
		t.Fatal(err)
	}

	transport := NewInMemoryVoteTransport()

	if err := transport.Register("node-B", nodeB); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = transport.RequestVote(
		ctx,
		"node-B",
		RequestVoteRequest{
			Term:        1,
			CandidateID: "node-A",
		},
	)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}

func TestInMemoryVoteTransportRejectsEmptyPeerID(t *testing.T) {
	transport := NewInMemoryVoteTransport()

	err := transport.Register("", nil)

	if !errors.Is(err, ErrInvalidPeerID) {
		t.Fatalf("error = %v, want ErrInvalidPeerID", err)
	}
}
