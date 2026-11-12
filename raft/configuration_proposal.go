package raft

import "errors"

var (
	ErrNotLeader = errors.New("raft node is not the leader")

	ErrLogStorageUnsupported = errors.New(
		"raft storage does not support persistent log storage",
	)

	ErrConfigurationChangeInProgress = errors.New(
		"raft configuration change already in progress",
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

	// Serialize proposals with committed-entry application.
	n.applyMu.Lock()
	defer n.applyMu.Unlock()

	n.mu.Lock()

	if n.state != StateLeader {
		n.mu.Unlock()
		return 0, ErrNotLeader
	}

	lastIndex := n.lastLogIndexLocked()

	if n.lastApplied > lastIndex {
		n.mu.Unlock()
		return 0, ErrInvalidLogState
	}

	// Reject another transition while a previous configuration
	// entry remains unapplied.
	// A durable indexed record means the configuration entry was
	// already applied, even if a crash occurred before lastApplied
	// was advanced. Do not mistake that entry for a pending change.
	var records []ConfigurationRecord

	if configurationStorage, ok := n.storage.(IndexedConfigurationStorage); ok {
		var err error
		records, err = configurationStorage.LoadConfigurationRecords()
		if err != nil {
			n.mu.Unlock()
			return 0, err
		}
	}

	if n.lastApplied < lastIndex {
		for index := n.lastApplied + 1; ; index++ {
			entry, exists := n.logEntryAt(index)
			if !exists {
				n.mu.Unlock()
				return 0, ErrInvalidLogState
			}

			if entry.Type == LogEntryConfiguration {
				recorded := false

				for _, record := range records {
					if record.Index != index {
						continue
					}

					if entry.Configuration == nil ||
						!equalVoterSets(
							record.Configuration.OldVoters,
							entry.Configuration.OldVoters,
						) ||
						!equalVoterSets(
							record.Configuration.NewVoters,
							entry.Configuration.NewVoters,
						) {
						n.mu.Unlock()
						return 0, ErrConfigurationIndexConflict
					}

					recorded = true
					break
				}

				if !recorded {
					n.mu.Unlock()
					return 0, ErrConfigurationChangeInProgress
				}
			}

			if index == lastIndex {
				break
			}
		}
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

	if lastIndex == ^uint64(0) ||
		n.snapshotIndex == ^uint64(0) {
		n.mu.Unlock()
		return 0, ErrInvalidLogState
	}

	index := lastIndex + 1

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

	// Persist before publishing.
	if err := logStorage.SaveLog(LogState{
		FirstIndex: n.snapshotIndex + 1,
		Entries:    candidate,
	}); err != nil {
		n.mu.Unlock()
		return 0, err
	}

	n.log = candidate
	n.lastLogIndex = index
	n.lastLogTerm = n.currentTerm

	if n.matchIndex == nil {
		n.matchIndex = make(map[string]uint64)
	}

	n.matchIndex[n.id] = index

	n.advanceCommitIndexLocked()

	n.mu.Unlock()

	// applyMu is already held. Use the existing locked helper.
	n.mu.RLock()
	stateMachine := n.stateMachine
	n.mu.RUnlock()

	if err := n.applyCommittedEntriesLocked(stateMachine); err != nil {
		return index, err
	}

	return index, nil
}
