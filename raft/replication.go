package raft

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	ErrNilAppendEntriesTransport = errors.New("append entries transport cannot be nil")
	ErrUnknownReplicationPeer    = errors.New("unknown replication peer")
	ErrInvalidNextIndex          = errors.New("invalid replication next index")
)

// ReplicateToPeer sends the log entries needed by one follower.
//
// It returns true when the follower confirms successful replication.
// The network request is performed without holding the node mutex.
func (n *Node) ReplicateToPeer(
	ctx context.Context,
	transport AppendEntriesTransport,
	peerID string,
) (bool, error) {
	if transport == nil {
		return false, ErrNilAppendEntriesTransport
	}

	if err := ctx.Err(); err != nil {
		return false, err
	}

	n.mu.Lock()

	if n.state != StateLeader {
		n.mu.Unlock()
		return false, nil
	}

	nextIndex, exists := n.nextIndex[peerID]
	if !exists {
		n.mu.Unlock()
		return false, ErrUnknownReplicationPeer
	}

	lastLogIndex := uint64(len(n.log))

	if nextIndex == 0 || nextIndex > lastLogIndex+1 {
		n.mu.Unlock()
		return false, ErrInvalidNextIndex
	}

	leaderTerm := n.currentTerm
	prevLogIndex := nextIndex - 1

	prevLogTerm, exists := n.termAt(prevLogIndex)
	if !exists {
		n.mu.Unlock()
		return false, ErrInvalidNextIndex
	}

	// Copy entries so the network request does not share mutable
	// command byte slices with the leader's local log.
	entries := make([]LogEntry, 0, lastLogIndex-prevLogIndex)

	for index := nextIndex; index <= lastLogIndex; index++ {
		entry := n.log[index-1]

		entries = append(entries, LogEntry{
			Term:    entry.Term,
			Command: append([]byte(nil), entry.Command...),
		})
	}

	req := AppendEntriesRequest{
		Term:         leaderTerm,
		LeaderID:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}

	n.mu.Unlock()

	// Never hold n.mu while performing network communication.
	response, err := transport.AppendEntries(ctx, peerID, req)
	if err != nil {
		return false, err
	}

	// A higher-term response means this node must step down.
	if response.Term > leaderTerm {
		if err := n.ObserveHigherTerm(response.Term); err != nil &&
			!errors.Is(err, ErrTermNotHigher) {
			return false, err
		}

		return false, nil
	}

	// Ignore responses from a different or outdated term.
	if response.Term != leaderTerm {
		return false, nil
	}

	n.mu.Lock()

	if n.state != StateLeader || n.currentTerm != leaderTerm {
		n.mu.Unlock()
		return false, nil
	}

	if !response.Success {
		n.recordReplicationFailureLocked(
			peerID,
			leaderTerm,
			nextIndex,
		)

		n.mu.Unlock()
		return false, nil
	}

	replicatedIndex := prevLogIndex + uint64(len(entries))

	if !n.recordReplicationSuccessLocked(
		peerID,
		leaderTerm,
		replicatedIndex,
	) {
		n.mu.Unlock()
		return false, nil
	}

	n.advanceCommitIndexLocked()

	n.mu.Unlock()

	// Apply committed entries only after releasing the Raft mutex.
	if err := n.applyCommittedEntriesIfConfigured(); err != nil {
		return true, err
	}

	return true, nil
}

// ReplicateToPeerUntilCaughtUp retries replication while the follower's
// log is behind the leader's log.
//
// It stops when replication succeeds, leadership is lost, no further
// progress can be made, or the context is cancelled.
func (n *Node) ReplicateToPeerUntilCaughtUp(
	ctx context.Context,
	transport AppendEntriesTransport,
	peerID string,
) (bool, error) {
	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}

		n.mu.RLock()

		if n.state != StateLeader {
			n.mu.RUnlock()
			return false, nil
		}

		previousNextIndex, exists := n.nextIndex[peerID]

		n.mu.RUnlock()

		if !exists {
			return false, ErrUnknownReplicationPeer
		}

		replicated, err := n.ReplicateToPeer(
			ctx,
			transport,
			peerID,
		)
		if err != nil {
			return false, err
		}

		if replicated {
			return true, nil
		}

		n.mu.RLock()

		stillLeader := n.state == StateLeader
		currentNextIndex := n.nextIndex[peerID]

		n.mu.RUnlock()

		if !stillLeader {
			return false, nil
		}

		// Stop if the previous attempt did not move the
		// replication position backwards.
		if currentNextIndex >= previousNextIndex {
			return false, nil
		}
	}
}

// ReplicateToAllPeers replicates the leader's log to every follower
// concurrently. Each follower is allowed to catch up independently.
func (n *Node) ReplicateToAllPeers(
	ctx context.Context,
	transport AppendEntriesTransport,
) (map[string]bool, error) {
	if transport == nil {
		return nil, ErrNilAppendEntriesTransport
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	n.mu.RLock()

	if n.state != StateLeader {
		n.mu.RUnlock()
		return map[string]bool{}, nil
	}

	peers := append([]string(nil), n.peers...)

	n.mu.RUnlock()

	results := make(map[string]bool, len(peers))

	var (
		wg       sync.WaitGroup
		resultMu sync.Mutex
		errs     []error
	)

	for _, peerID := range peers {
		wg.Add(1)

		go func(peer string) {
			defer wg.Done()

			replicated, err := n.ReplicateToPeerUntilCaughtUp(
				ctx,
				transport,
				peer,
			)

			resultMu.Lock()
			defer resultMu.Unlock()

			results[peer] = replicated

			if err != nil {
				errs = append(
					errs,
					fmt.Errorf("replicate to %s: %w", peer, err),
				)
			}
		}(peerID)
	}

	wg.Wait()

	return results, errors.Join(errs...)
}
