package raft

import (
	"errors"
	"fmt"
)

var (
	ErrSnapshotStorageUnavailable = errors.New(
		"snapshot storage is unavailable",
	)

	ErrSnapshotStateMachineUnsupported = errors.New(
		"state machine does not support snapshots",
	)

	ErrInvalidSnapshotIndex = errors.New(
		"snapshot index must be greater than the current snapshot index and no greater than last applied",
	)
)

// CreateSnapshot exports the state machine state, persists it,
// and compacts the log entries covered by the snapshot.
//
// The snapshot is persisted before any log entries are removed.
func (n *Node) CreateSnapshot() error {
	// Serialize snapshot creation with state-machine application.
	n.applyMu.Lock()
	defer n.applyMu.Unlock()

	n.mu.RLock()
	stateMachine := n.stateMachine
	storage, storageOK := n.storage.(SnapshotStorage)
	snapshotIndex := n.lastApplied
	currentSnapshotIndex := n.snapshotIndex
	n.mu.RUnlock()

	if stateMachine == nil {
		return ErrNilStateMachine
	}

	if !storageOK {
		return ErrSnapshotStorageUnavailable
	}

	snapshotMachine, ok := stateMachine.(SnapshotStateMachine)
	if !ok {
		return ErrSnapshotStateMachineUnsupported
	}

	if snapshotIndex <= currentSnapshotIndex {
		return ErrInvalidSnapshotIndex
	}

	// Find the term belonging to the last applied entry.
	n.mu.RLock()
	snapshotTerm, exists := n.termAt(snapshotIndex)
	n.mu.RUnlock()

	if !exists || snapshotTerm == 0 {
		return fmt.Errorf(
			"%w: cannot find term at index %d",
			ErrInvalidSnapshot,
			snapshotIndex,
		)
	}

	// Export state while application is paused by applyMu.
	data, err := snapshotMachine.Snapshot()
	if err != nil {
		return fmt.Errorf("export state machine snapshot: %w", err)
	}

	snapshot := Snapshot{
		LastIncludedIndex: snapshotIndex,
		LastIncludedTerm:  snapshotTerm,
		Data:              append([]byte(nil), data...),
	}

	if err := validateSnapshot(snapshot); err != nil {
		return err
	}

	// Critical ordering: persist before compacting the log.
	if err := storage.SaveSnapshot(snapshot); err != nil {
		return fmt.Errorf("persist snapshot: %w", err)
	}

	// Compact only after persistence succeeds.
	n.mu.Lock()
	defer n.mu.Unlock()

	// Keep all entries after the snapshot index.
	offset := snapshotIndex - n.snapshotIndex

	if offset > uint64(len(n.log)) {
		return fmt.Errorf(
			"%w: snapshot index %d exceeds retained log",
			ErrInvalidSnapshot,
			snapshotIndex,
		)
	}

	n.log = append([]LogEntry(nil), n.log[offset:]...)

	n.snapshot = cloneSnapshot(snapshot)
	n.snapshotIndex = snapshot.LastIncludedIndex
	n.snapshotTerm = snapshot.LastIncludedTerm

	// Recalculate the absolute last-log metadata.
	n.lastLogIndex = n.lastLogIndexLocked()

	if len(n.log) > 0 {
		n.lastLogTerm = n.log[len(n.log)-1].Term
	} else {
		n.lastLogTerm = n.snapshotTerm
	}

	return nil
}
