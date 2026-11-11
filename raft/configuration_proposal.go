package raft

import "errors"

var (
	ErrNotLeader = errors.New("raft node is not the leader")

	ErrLogStorageUnsupported = errors.New(
		"raft storage does not support persistent log storage",
	)
)

// ProposeConfiguration appends a membership-change entry to the
// leader's log.
//
// The entry is persisted before it is published in memory.
// The active configuration changes only after the entry is committed
// and applied.
func (n *Node) ProposeConfiguration(
	configuration ConfigurationState,
) (uint64, error) {
	if n == nil {
		return 0, ErrNotLeader
	}

	n.mu.Lock()

	if n.state != StateLeader {
		n.mu.Unlock()
		return 0, ErrNotLeader
	}

	if err := ValidateConfigurationTransition(
		n.configuration,
		configuration,
	); err != nil {
		n.mu.Unlock()
		return 0, err
	}

	logStorage, ok := n.storage.(LogStorage)
	if !ok {
		n.mu.Unlock()
		return 0, ErrLogStorageUnsupported
	}

	lastIndex := n.lastLogIndexLocked()

	if lastIndex == ^uint64(0) ||
		n.snapshotIndex == ^uint64(0) {
		n.mu.Unlock()
		return 0, ErrInvalidLogState
	}

	index := lastIndex + 1

	// Build a separate candidate log. Do not modify the live log
	// until persistent storage confirms the write.
	candidate := cloneLogState(LogState{
		FirstIndex: n.snapshotIndex + 1,
		Entries:    n.log,
	}).Entries

	configurationCopy := configuration.Clone()

	candidate = append(candidate, LogEntry{
		Term:          n.currentTerm,
		Type:          LogEntryConfiguration,
		Configuration: &configurationCopy,
	})

	// Persistence must succeed before publishing the new entry.
	if err := logStorage.SaveLog(LogState{
		FirstIndex: n.snapshotIndex + 1,
		Entries:    candidate,
	}); err != nil {
		n.mu.Unlock()
		return 0, err
	}

	// Publish the durable entry in memory.
	n.log = candidate
	n.lastLogIndex = index
	n.lastLogTerm = n.currentTerm

	if n.matchIndex == nil {
		n.matchIndex = make(map[string]uint64)
	}

	// The leader already has its own durable copy.
	n.matchIndex[n.id] = index

	// A single-node cluster can commit using its own vote.
	n.advanceCommitIndexLocked()

	n.mu.Unlock()

	// Apply immediately if this entry became committed.
	if err := n.applyCommittedEntriesIfConfigured(); err != nil {
		return index, err
	}

	return index, nil
}
