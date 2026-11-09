package raft

import (
	"context"
	"errors"
	"testing"
)

func TestInMemoryAppendEntriesTransportDeliversRequest(t *testing.T) {
	follower, err := NewNode("follower-1", []string{"leader-1"})
	if err != nil {
		t.Fatal(err)
	}

	transport := NewInMemoryAppendEntriesTransport()

	if err := transport.Register("follower-1", follower); err != nil {
		t.Fatal(err)
	}

	response, err := transport.AppendEntries(
		context.Background(),
		"follower-1",
		AppendEntriesRequest{
			Term:     1,
			LeaderID: "leader-1",
			Entries: []LogEntry{
				{Term: 1, Command: []byte("create-job")},
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if !response.Success {
		t.Fatal("expected successful AppendEntries response")
	}

	if len(follower.log) != 1 {
		t.Fatalf("expected one replicated entry, got %d", len(follower.log))
	}
}

func TestInMemoryAppendEntriesTransportRejectsUnavailablePeer(t *testing.T) {
	transport := NewInMemoryAppendEntriesTransport()

	_, err := transport.AppendEntries(
		context.Background(),
		"missing-node",
		AppendEntriesRequest{
			Term:     1,
			LeaderID: "leader-1",
		},
	)

	if !errors.Is(err, ErrPeerUnavailable) {
		t.Fatalf("expected ErrPeerUnavailable, got %v", err)
	}
}

func TestInMemoryAppendEntriesTransportRejectsCanceledContext(t *testing.T) {
	transport := NewInMemoryAppendEntriesTransport()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := transport.AppendEntries(
		ctx,
		"follower-1",
		AppendEntriesRequest{
			Term:     1,
			LeaderID: "leader-1",
		},
	)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestInMemoryAppendEntriesTransportRejectsInvalidRegistration(t *testing.T) {
	transport := NewInMemoryAppendEntriesTransport()

	if err := transport.Register("", nil); !errors.Is(err, ErrInvalidPeerID) {
		t.Fatalf("expected ErrInvalidPeerID, got %v", err)
	}

	if err := transport.Register("follower-1", nil); !errors.Is(err, ErrPeerUnavailable) {
		t.Fatalf("expected ErrPeerUnavailable, got %v", err)
	}
}
