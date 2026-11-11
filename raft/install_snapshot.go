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
	Configuration     ConfigurationState
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

	incoming := Snapshot{
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm:  req.LastIncludedTerm,
		Configuration:     req.Configuration.Clone(),
		Data:              append([]byte(nil), req.Data...),
	}

	if req.Term == 0 ||
		req.LeaderID == "" ||
		validateSnapshot(incoming) != nil ||
		incoming.LastIncludedIndex == 0 ||
		incoming.LastIncludedIndex == ^uint64(0) {
		return response, ErrInvalidInstallSnapshot
	}

	// Reject outdated leaders.
	if req.Term < n.currentTerm {
		return response, nil
	}

	// Persist a higher term before updating memory.
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

	if incoming.LastIncludedIndex <= n.snapshotIndex {
		return response, ErrSnapshotBehind
	}

	if incoming.LastIncludedIndex < n.commitIndex {
		return response, ErrSnapshotBehind
	}

	if incoming.LastIncludedTerm > req.Term {
		return response, ErrInvalidInstallSnapshot
	}

	// Validate state-machine support before changing persistent state.
	var restorer SnapshotRestorer

	if n.stateMachine != nil {
		var supported bool

		restorer, supported = n.stateMachine.(SnapshotRestorer)
		if !supported {
			return response, ErrSnapshotRestoreUnsupported
		}
	}

	snapshotStorage, ok := n.storage.(SnapshotStorage)
	if !ok {
		return response, ErrSnapshotStorageUnavailable
	}

	// Preserve the local suffix only when its boundary term matches.
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

	if uint64(len(retainedLog)) >
		^uint64(0)-incoming.LastIncludedIndex {
		return response, ErrInvalidLogState
	}

	// Load the previous persisted log so a failed installation
	// can attempt to restore it.
	logStorage, hasLogStorage := n.storage.(LogStorage)

	var previousLog LogState

	if hasLogStorage {
		var err error

		previousLog, err = logStorage.LoadLog()
		if err != nil {
			return response, fmt.Errorf(
				"load previous log before snapshot installation: %w",
				err,
			)
		}

		if err := validateLogState(previousLog); err != nil {
			return response, err
		}
	}

	previousSnapshot := cloneSnapshot(n.snapshot)

	// Roll back both persistent records if a later installation
	// step fails.

	rollback := func(cause error) error {
		var rollbackErrors []error

		// Restore the previous log first.
		logRollbackSucceeded := true

		if hasLogStorage {
			if err := logStorage.SaveLog(previousLog); err != nil {
				logRollbackSucceeded = false

				rollbackErrors = append(
					rollbackErrors,
					fmt.Errorf("rollback persisted log: %w", err),
				)
			}
		}

		// Restore the previous snapshot only when the previous
		// log has been restored successfully.
		if logRollbackSucceeded {
			if err := snapshotStorage.SaveSnapshot(previousSnapshot); err != nil {
				rollbackErrors = append(
					rollbackErrors,
					fmt.Errorf("rollback persisted snapshot: %w", err),
				)
			}
		}

		return errors.Join(
			append([]error{cause}, rollbackErrors...)...,
		)
	}

	// Persist the snapshot before changing the state machine.
	if err := snapshotStorage.SaveSnapshot(incoming); err != nil {
		return response, err
	}

	// Persist the matching retained log suffix.
	if hasLogStorage {
		logState := LogState{
			FirstIndex: incoming.LastIncludedIndex + 1,
			Entries:    retainedLog,
		}

		if err := logStorage.SaveLog(logState); err != nil {
			return response, rollback(
				fmt.Errorf("persist retained log: %w", err),
			)
		}
	}

	// Restore the state machine only after both persistent records
	// have been saved successfully.
	if restorer != nil {
		if err := restorer.Restore(
			append([]byte(nil), incoming.Data...),
		); err != nil {
			return response, rollback(
				fmt.Errorf("restore incoming snapshot: %w", err),
			)
		}
	}

	// Publish the installed snapshot and retained log in memory.
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

	// Publish the membership configuration only after the
	// snapshot and state-machine restoration have succeeded.
	n.configuration = incoming.Configuration.Clone()
	n.peers = configurationPeerIDs(n.id, n.configuration)

	// A node removed from the voter set must not remain leader.
	if !n.configuration.IsVoter(n.id) {
		n.state = StateFollower
	}

	response.Success = true

	return response, nil
}
