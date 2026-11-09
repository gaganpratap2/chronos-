package raft

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestReplicationLoopRejectsInvalidConfiguration(t *testing.T) {
	tests := []struct {
		name string
		loop ReplicationLoop
		want error
	}{
		{
			name: "nil node",
			loop: ReplicationLoop{
				Transport:      NewInMemoryAppendEntriesTransport(),
				Interval:       time.Second,
				RequestTimeout: time.Second,
			},
			want: ErrInvalidStorage,
		},
		{
			name: "nil transport",
			loop: ReplicationLoop{
				Node:           &Node{},
				Interval:       time.Second,
				RequestTimeout: time.Second,
			},
			want: ErrNilAppendEntriesTransport,
		},
		{
			name: "invalid interval",
			loop: ReplicationLoop{
				Node:           &Node{},
				Transport:      NewInMemoryAppendEntriesTransport(),
				RequestTimeout: time.Second,
			},
			want: ErrInvalidHeartbeatInterval,
		},
		{
			name: "invalid request timeout",
			loop: ReplicationLoop{
				Node:      &Node{},
				Transport: NewInMemoryAppendEntriesTransport(),
				Interval:  time.Second,
			},
			want: ErrInvalidRequestTimeout,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.loop.Run(context.Background())

			if !errors.Is(err, tt.want) {
				t.Fatalf("expected %v, got %v", tt.want, err)
			}
		})
	}
}

func TestReplicationLoopStopsWhenContextCancelled(t *testing.T) {
	node, err := NewNode("node-A", nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	loop := ReplicationLoop{
		Node:           node,
		Transport:      NewInMemoryAppendEntriesTransport(),
		Interval:       10 * time.Millisecond,
		RequestTimeout: time.Second,
	}

	err = loop.Run(ctx)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

type blockingAppendEntriesTransport struct{}

func (blockingAppendEntriesTransport) AppendEntries(
	ctx context.Context,
	peerID string,
	req AppendEntriesRequest,
) (AppendEntriesResponse, error) {
	<-ctx.Done()

	return AppendEntriesResponse{}, ctx.Err()
}

func TestTimeoutTransportCancelsSlowRequest(t *testing.T) {
	transport := &timeoutAppendEntriesTransport{
		transport: blockingAppendEntriesTransport{},
		timeout:   20 * time.Millisecond,
	}

	start := time.Now()

	_, err := transport.AppendEntries(
		context.Background(),
		"node-B",
		AppendEntriesRequest{},
	)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}

	if time.Since(start) > time.Second {
		t.Fatal("request timeout took too long")
	}
}
