package raft

import "fmt"

// ErrSnapshotRestoreUnsupported indicates that a state machine
// cannot restore a persisted Raft snapshot.
var ErrSnapshotRestoreUnsupported = fmt.Errorf(
	"state machine does not support snapshot restoration",
)

// SetStateMachine registers the state machine, restores any persisted
// snapshot, and applies committed entries after the snapshot boundary.
func (n *Node) SetStateMachine(
	stateMachine StateMachine,
) error {
	if stateMachine == nil {
		return ErrNilStateMachine
	}

	// Serialize restoration with application and snapshot creation.
	n.applyMu.Lock()
	defer n.applyMu.Unlock()

	n.mu.RLock()
	snapshot := cloneSnapshot(n.snapshot)
	n.mu.RUnlock()

	// Restore the snapshot before applying later log entries.
	if snapshot.LastIncludedIndex > 0 {
		restorer, ok := stateMachine.(SnapshotRestorer)
		if !ok {
			return ErrSnapshotRestoreUnsupported
		}

		if err := restorer.Restore(
			append([]byte(nil), snapshot.Data...),
		); err != nil {
			return fmt.Errorf("restore raft snapshot: %w", err)
		}
	}

	n.mu.Lock()
	n.stateMachine = stateMachine
	n.mu.Unlock()

	// applyMu is already held.
	return n.applyCommittedEntriesLocked(stateMachine)
}

// applyCommittedEntriesIfConfigured applies committed entries,
// including configuration entries, when possible.
func (n *Node) applyCommittedEntriesIfConfigured() error {
	n.applyMu.Lock()
	defer n.applyMu.Unlock()

	n.mu.RLock()
	stateMachine := n.stateMachine
	n.mu.RUnlock()

	return n.applyCommittedEntriesLocked(stateMachine)
}
