package raft

import (
	"errors"
	"math"
)

var (
	ErrAlreadyLeader = errors.New("raft node is already leader")
	ErrNotCandidate  = errors.New("raft node is not a candidate")
	ErrInvalidTerm   = errors.New("raft term must be greater than zero")
	ErrTermExhausted = errors.New("raft term has reached its maximum value")
	ErrTermNotHigher = errors.New("received term is not higher")
)

// start election

/*

Correct persistence sequence
1. Calculate the new term and vote.
2. Save them to storage.
3. Only after successful storage, update memory.
4. Only later, send election messages.

*/

func (n *Node) StartElection() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state == StateLeader {
		return ErrAlreadyLeader
	}

	if n.currentTerm == math.MaxUint64 {
		return ErrTermExhausted
	}

	nextTerm := n.currentTerm + 1

	hardState := HardState{
		CurrentTerm: nextTerm,
		VotedFor:    n.id,
	}

	if err := n.storage.SaveHardState(hardState); err != nil {
		return err
	}

	n.currentTerm = nextTerm
	n.state = StateCandidate
	n.votedFor = n.id

	return nil

}

func (n *Node) BecomeLeader() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != StateCandidate {
		return ErrNotCandidate
	}

	n.state = StateLeader
	return nil
}

// upgrade the team to higher term

func (n *Node) ObserveHigherTerm(term uint64) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if term == 0 {
		return ErrInvalidTerm
	}

	if term <= n.currentTerm {
		return ErrTermNotHigher
	}

	hardState := HardState{
		CurrentTerm: term,
		VotedFor:    "",
	}

	// Persist before changing in-memory state.
	if err := n.storage.SaveHardState(hardState); err != nil {
		return err
	}

	n.currentTerm = term
	n.state = StateFollower
	n.votedFor = ""

	return nil
}
