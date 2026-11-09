package raft

import (
	"errors"
	"fmt"
)

var (
	ErrNilStateMachine = errors.New(
		"state machine cannot be nil",
	)

	ErrCommittedEntryMissing = errors.New(
		"committed log entry is missing",
	)
)

// LastApplied returns the highest log index successfully applied.
func (n *Node) LastApplied() uint64 {
	n.applyMu.Lock()
	defer n.applyMu.Unlock()

	return n.lastApplied
}

// ApplyCommittedEntries applies committed entries in log order.
func (n *Node) ApplyCommittedEntries(
	stateMachine StateMachine,
) error {
	if stateMachine == nil {
		return ErrNilStateMachine
	}

	n.applyMu.Lock()
	defer n.applyMu.Unlock()

	for {
		n.mu.RLock()

		if n.lastApplied >= n.commitIndex {
			n.mu.RUnlock()
			return nil
		}

		nextIndex := n.lastApplied + 1

		if nextIndex > uint64(len(n.log)) {
			n.mu.RUnlock()
			return ErrCommittedEntryMissing
		}

		entry := n.log[nextIndex-1]
		command := append([]byte(nil), entry.Command...)

		n.mu.RUnlock()

		if err := stateMachine.Apply(nextIndex, command); err != nil {
			return fmt.Errorf(
				"apply log entry %d: %w",
				nextIndex,
				err,
			)
		}

		n.lastApplied = nextIndex
	}
}
