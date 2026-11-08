package raft

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrNilElectionNode = errors.New(
		"election coordinator requires a node",
	)
	ErrNilVoteTransport = errors.New(
		"election coordinator requires a vote transport",
	)
	ErrInvalidElectionAttempts = errors.New(
		"maximum election attempts must be positive")
)

// ElectionCoordinator manages one Raft election.
type ElectionCoordinator struct {
	node      *Node
	transport VoteTransport

	// Prevents overlapping elections through this coordinator.
	mu sync.Mutex
}

// NewElectionCoordinator creates an election coordinator.
func NewElectionCoordinator(
	node *Node,
	transport VoteTransport,
) (*ElectionCoordinator, error) {
	if node == nil {
		return nil, ErrNilElectionNode
	}

	if transport == nil {
		return nil, ErrNilVoteTransport
	}

	return &ElectionCoordinator{
		node:      node,
		transport: transport,
	}, nil
}

type voteResult struct {
	response RequestVoteResponse
	err      error
}

// RunElection starts an election and collects votes.
//
// It returns true if this node becomes Leader.
// It returns false if the election finishes without a majority
// or the node discovers a higher term.
func (e *ElectionCoordinator) RunElection(
	ctx context.Context,
) (bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if ctx == nil {
		return false, errors.New("election context cannot be nil")
	}

	if err := ctx.Err(); err != nil {
		return false, err
	}

	// Step 1: Become Candidate and persist the self-vote.
	if err := e.node.StartElection(); err != nil {
		return false, err
	}

	// Step 2: Capture election information.
	e.node.mu.RLock()

	electionTerm := e.node.currentTerm
	candidateID := e.node.id
	lastLogIndex := e.node.lastLogIndex
	lastLogTerm := e.node.lastLogTerm

	peers := append([]string(nil), e.node.peers...)

	e.node.mu.RUnlock()

	// The Candidate automatically votes for itself.
	votes := 1

	clusterSize := len(peers) + 1
	majority := clusterSize/2 + 1

	// A one-node cluster needs only its own vote.
	if votes >= majority {
		if err := e.node.becomeLeaderIfMajority(
			electionTerm,
			votes,
		); err != nil {
			return false, err
		}

		return true, nil
	}

	// Step 3: Prepare the RequestVote message.
	request := RequestVoteRequest{
		Term:         electionTerm,
		CandidateID:  candidateID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	results := make(chan voteResult, len(peers))

	// Step 4: Request votes concurrently.
	for _, peerID := range peers {
		go func(id string) {
			response, err := e.transport.RequestVote(
				ctx,
				id,
				request,
			)

			results <- voteResult{
				response: response,
				err:      err,
			}
		}(peerID)
	}

	// Step 5: Collect responses.
	for remaining := len(peers); remaining > 0; remaining-- {
		select {
		case <-ctx.Done():
			return false, ctx.Err()

		case result := <-results:
			// An unreachable peer cannot contribute a vote.
			if result.err != nil {
				continue
			}

			// A higher term means this election is superseded.
			if result.response.Term > electionTerm {
				err := e.node.ObserveHigherTerm(
					result.response.Term,
				)

				if err != nil &&
					!errors.Is(err, ErrTermNotHigher) {
					return false, err
				}

				return false, nil
			}

			// Count only granted votes from the current term.
			if result.response.Term == electionTerm &&
				result.response.VoteGranted {
				votes++
			}

			// Step 6: Check whether a majority was reached.
			if votes >= majority {
				if err := e.node.becomeLeaderIfMajority(
					electionTerm,
					votes,
				); err != nil {
					return false, err
				}

				return true, nil
			}
		}
	}

	// All peer responses arrived, but there was no majority.
	return false, nil
}

// RunElectionWithRetry retries unsuccessful elections using randomized delays.
// maxAttempts includes the initial election attempt.
func (e *ElectionCoordinator) RunElectionWithRetry(
	ctx context.Context,
	config ElectionConfig,
	maxAttempts int,
) (bool, error) {
	if ctx == nil {
		return false, errors.New("election context cannot be nil")
	}

	if maxAttempts <= 0 {
		return false, ErrInvalidElectionAttempts
	}

	if err := config.Validate(); err != nil {
		return false, err
	}

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return false, err
		}

		elected, err := e.RunElection(ctx)
		if err != nil {
			return false, err
		}

		if elected {
			return true, nil
		}

		// A higher-term response may have made this node a follower.
		// A follower should not continue this candidate's retry loop.
		if e.node.State() != StateCandidate {
			return false, nil
		}

		if attempt == maxAttempts {
			return false, nil
		}

		delay, err := config.RandomTimeout()
		if err != nil {
			return false, err
		}

		timer := time.NewTimer(delay)

		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return false, ctx.Err()

		case <-timer.C:
			// Start the next election attempt.
		}
	}

	return false, nil
}
