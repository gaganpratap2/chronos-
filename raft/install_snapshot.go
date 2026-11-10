package raft

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidInstallSnapshot = errors.New(
		"invalid install snapshot request",
	)
	ErrSnapshotBehind = errors.New(
		"incoming snapshot is not newer than local snapshot",
	)
)

// InstallSnapshotRequest carries a complete snapshot from the leader.
type InstallSnapshotRequest struct {
	Term              uint64
	LeaderID          string
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

// InstallSnapshotResponse reports whether the follower installed
// the snapshot and returns its current term.
type InstallSnapshotResponse struct {
	Term    uint64
	Success bool
}

// InstallSnapshot installs a snapshot received from the current leader.
func (n *Node) InstallSnapshot(
	req InstallSnapshotRequest,
) (InstallSnapshotResponse, error) {
	n.applyMu.Lock()
	defer n.applyMu.Unlock()

	n.mu.Lock()
	defer n.mu.Unlock()

	response := InstallSnapshotResponse{
		Term:    n.currentTerm,
		Success: false,
	}

	// Validate the request before changing persistent or in-memory state.
	incoming := Snapshot{
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm:  req.LastIncludedTerm,
		Data:              append([]byte(nil), req.Data...),
	}

	if req.Term == 0 ||
		req.LeaderID == "" ||
		validateSnapshot(incoming) != nil ||
		incoming.LastIncludedIndex == 0 {
		return response, ErrInvalidInstallSnapshot
	}

	// Reject outdated leaders.
	if req.Term < n.currentTerm {
		return response, nil
	}

	// A newer leader term must be persisted before updating memory.
	if req.Term > n.currentTerm {
		hardState := HardState{
			CurrentTerm: req.Term,
			VotedFor:    "",
		}

		if err := n.storage.SaveHardState(hardState); err != nil {
			return response, err
		}

		n.currentTerm = req.Term
		n.votedFor = ""
	}

	n.state = StateFollower
	response.Term = n.currentTerm

	// An older or duplicate snapshot cannot replace newer local state.
	if incoming.LastIncludedIndex <= n.snapshotIndex {
		return response, ErrSnapshotBehind
	}

	// A snapshot cannot roll back already committed entries.
	if incoming.LastIncludedIndex < n.commitIndex {
		return response, ErrSnapshotBehind
	}

	// A snapshot cannot move beyond the leader's current term.
	if incoming.LastIncludedTerm > req.Term {
		return response, ErrInvalidInstallSnapshot
	}

	// Restore the state machine before publishing the new snapshot.
	if n.stateMachine != nil {
		snapshotStorage, ok := n.storage.(SnapshotStorage)
		if !ok {
			return response, ErrSnapshotStorageUnavailable
		}

		var restorer SnapshotRestorer

		if n.stateMachine != nil {
			var supported bool

			restorer, supported = n.stateMachine.(SnapshotRestorer)
			if !supported {
				return response, ErrSnapshotRestoreUnsupported
			}
		}

		// Keep the previous snapshot so we can roll back persistence
		// if restoring the incoming snapshot fails.
		previousSnapshot := cloneSnapshot(n.snapshot)

		// Persist before modifying the state machine.
		if err := snapshotStorage.SaveSnapshot(incoming); err != nil {
			return response, err
		}

		// Restore the state machine only after persistence succeeds.
		if restorer != nil {
			if err := restorer.Restore(
				append([]byte(nil), incoming.Data...),
			); err != nil {
				restoreErr := fmt.Errorf(
					"restore incoming snapshot: %w",
					err,
				)

				// Restore the previous persisted snapshot.
				if rollbackErr := snapshotStorage.SaveSnapshot(
					previousSnapshot,
				); rollbackErr != nil {
					return response, errors.Join(
						restoreErr,
						fmt.Errorf(
							"rollback persisted snapshot: %w",
							rollbackErr,
						),
					)
				}

				return response, restoreErr
			}
		}
	}

	// Snapshot persistence must succeed before changing the log.
	snapshotStorage, ok := n.storage.(SnapshotStorage)
	if !ok {
		return response, ErrSnapshotStorageUnavailable
	}

	if err := snapshotStorage.SaveSnapshot(incoming); err != nil {
		return response, err
	}

	// Preserve the suffix only if the local log agrees at the boundary.
	var retainedLog []LogEntry

	localTerm, exists := n.termAt(incoming.LastIncludedIndex)
	if exists && localTerm == incoming.LastIncludedTerm {
		offset := incoming.LastIncludedIndex - n.snapshotIndex

		if offset <= uint64(len(n.log)) {
			retainedLog = append(
				[]LogEntry(nil),
				n.log[offset:]...,
			)
		}
	}

	n.snapshot = cloneSnapshot(incoming)
	n.snapshotIndex = incoming.LastIncludedIndex
	n.snapshotTerm = incoming.LastIncludedTerm
	n.log = retainedLog

	n.lastLogIndex = n.lastLogIndexLocked()
	n.lastLogTerm = n.snapshotTerm

	if len(n.log) > 0 {
		n.lastLogTerm = n.log[len(n.log)-1].Term
	}

	n.commitIndex = incoming.LastIncludedIndex
	n.lastApplied = incoming.LastIncludedIndex

	response.Success = true

	return response, nil
}
