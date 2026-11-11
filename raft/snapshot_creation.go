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

	if snapshotIndex == ^uint64(0) {
		return ErrInvalidSnapshotIndex
	}

	// Keep the node log stable while persisting the snapshot and
	// its corresponding retained log.
	n.mu.Lock()
	defer n.mu.Unlock()

	if snapshotIndex <= n.snapshotIndex ||
		snapshotIndex > n.lastApplied {
		return ErrInvalidSnapshotIndex
	}

	currentTerm, exists := n.termAt(snapshotIndex)
	if !exists || currentTerm != snapshotTerm {
		return fmt.Errorf(
			"%w: snapshot boundary changed during creation",
			ErrInvalidSnapshot,
		)
	}

	offset := snapshotIndex - n.snapshotIndex

	if offset > uint64(len(n.log)) {
		return fmt.Errorf(
			"%w: snapshot index %d exceeds retained log",
			ErrInvalidSnapshot,
			snapshotIndex,
		)
	}

	retainedLog := append(
		[]LogEntry(nil),
		n.log[offset:]...,
	)

	if uint64(len(retainedLog)) > ^uint64(0)-snapshotIndex {
		return ErrInvalidSnapshot
	}

	previousSnapshot := cloneSnapshot(n.snapshot)

	// Capture the previous persisted log before modifying either
	// durable record. This gives rollback an exact prior state.
	logStorage, hasLogStorage := n.storage.(LogStorage)

	var previousLog LogState

	if hasLogStorage {
		previousLog, err = logStorage.LoadLog()
		if err != nil {
			return fmt.Errorf(
				"load previous log before snapshot creation: %w",
				err,
			)
		}

		if err := validateLogState(previousLog); err != nil {
			return err
		}
	}

	// First persist the snapshot. Recovery can reconcile an old
	// log with this newer snapshot if the process crashes afterward.
	if err := storage.SaveSnapshot(snapshot); err != nil {
		return fmt.Errorf("persist snapshot: %w", err)
	}

	// Persist the compacted log before publishing in-memory state.
	if hasLogStorage {
		logState := LogState{
			FirstIndex: snapshotIndex + 1,
			Entries:    retainedLog,
		}

		if err := logStorage.SaveLog(logState); err != nil {
			logErr := fmt.Errorf(
				"persist compacted log: %w",
				err,
			)

			// Restore the old log first. Never restore the old
			// snapshot if the old log could not be recovered.
			if rollbackErr := logStorage.SaveLog(previousLog); rollbackErr != nil {
				return errors.Join(
					logErr,
					fmt.Errorf(
						"rollback persisted log: %w",
						rollbackErr,
					),
				)
			}

			// The old log is safe again, so restoring the old
			// snapshot cannot create a snapshot/log gap.
			if rollbackErr := storage.SaveSnapshot(previousSnapshot); rollbackErr != nil {
				return errors.Join(
					logErr,
					fmt.Errorf(
						"rollback persisted snapshot: %w",
						rollbackErr,
					),
				)
			}

			return logErr
		}
	}

	// Both persistence operations succeeded. Publish the new state.
	n.log = retainedLog
	n.snapshot = cloneSnapshot(snapshot)
	n.snapshotIndex = snapshot.LastIncludedIndex
	n.snapshotTerm = snapshot.LastIncludedTerm

	n.lastLogIndex = n.lastLogIndexLocked()

	if len(n.log) > 0 {
		n.lastLogTerm = n.log[len(n.log)-1].Term
	} else {
		n.lastLogTerm = n.snapshotTerm
	}

	return nil
}
