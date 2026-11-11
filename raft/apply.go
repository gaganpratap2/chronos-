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

	return n.applyCommittedEntriesLocked(stateMachine)
}

// applyCommittedEntriesLocked applies committed entries.
// The caller must hold applyMu.
func (n *Node) applyCommittedEntriesLocked(
	stateMachine StateMachine,
) error {
	for {
		n.mu.RLock()

		if n.lastApplied >= n.commitIndex {
			n.mu.RUnlock()
			return nil
		}

		nextIndex := n.lastApplied + 1

		entry, exists := n.logEntryAt(nextIndex)
		if !exists {
			n.mu.RUnlock()
			return ErrCommittedEntryMissing
		}

		entry = cloneLogEntry(entry)

		n.mu.RUnlock()

		switch entry.Type {
		case "", LogEntryCommand:
			// A command cannot be skipped when no state machine
			// is registered. Leave lastApplied unchanged.
			if stateMachine == nil {
				return nil
			}

			if err := stateMachine.Apply(
				nextIndex,
				append([]byte(nil), entry.Command...),
			); err != nil {
				return fmt.Errorf(
					"apply log entry %d: %w",
					nextIndex,
					err,
				)
			}

		case LogEntryConfiguration:
			if entry.Configuration == nil {
				return ErrInvalidLogState
			}

			if err := n.applyConfigurationEntry(
				nextIndex,
				*entry.Configuration,
			); err != nil {
				return fmt.Errorf(
					"apply configuration entry %d: %w",
					nextIndex,
					err,
				)
			}

		default:
			return ErrInvalidLogState
		}

		// Advance only after the command or configuration
		// has been successfully applied.
		n.lastApplied = nextIndex
	}
}

// applyConfigurationEntry durably applies a committed configuration.
// The caller must hold applyMu.
// applyConfigurationEntry durably applies a committed configuration.
// The caller must hold applyMu.
func (n *Node) applyConfigurationEntry(
	index uint64,
	configuration ConfigurationState,
) error {
	if index == 0 {
		return ErrInvalidConfigurationIndex
	}

	if err := configuration.Validate(); err != nil {
		return err
	}

	configurationStorage, ok := n.storage.(IndexedConfigurationStorage)
	if !ok {
		return ErrConfigurationStorageUnsupported
	}

	configuration = configuration.Clone()

	// Check whether this exact configuration was already persisted.
	// This makes replay safe if a crash happened after persistence
	// but before lastApplied was advanced.
	records, err := configurationStorage.LoadConfigurationRecords()
	if err != nil {
		return fmt.Errorf(
			"load configuration records: %w",
			err,
		)
	}

	alreadyRecorded := false

	for _, record := range records {
		if record.Index != index {
			continue
		}

		if !equalVoterSets(
			record.Configuration.OldVoters,
			configuration.OldVoters,
		) || !equalVoterSets(
			record.Configuration.NewVoters,
			configuration.NewVoters,
		) {
			return ErrConfigurationIndexConflict
		}

		alreadyRecorded = true
		break
	}

	// A previously persisted record is a replay, not a new transition.
	if !alreadyRecorded {
		n.mu.RLock()
		currentConfiguration := n.configuration.Clone()
		n.mu.RUnlock()

		if err := ValidateConfigurationTransition(
			currentConfiguration,
			configuration,
		); err != nil {
			return err
		}
	}

	// Persist the configuration with its committed Raft log index.
	// An identical record at this index is idempotent.
	if err := configurationStorage.SaveConfigurationAt(
		index,
		configuration,
	); err != nil {
		return fmt.Errorf(
			"persist configuration at index %d: %w",
			index,
			err,
		)
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	newPeers := configurationPeerIDs(n.id, configuration)
	lastLogIndex := n.lastLogIndexLocked()

	// Preserve replication progress for peers that remain members.
	if n.nextIndex != nil {
		nextIndex := make(map[string]uint64, len(newPeers))

		for _, peerID := range newPeers {
			if previous, exists := n.nextIndex[peerID]; exists {
				nextIndex[peerID] = previous
			} else {
				nextIndex[peerID] = lastLogIndex + 1
			}
		}

		n.nextIndex = nextIndex
	}

	if n.matchIndex != nil {
		matchIndex := make(map[string]uint64, len(newPeers)+1)

		matchIndex[n.id] = lastLogIndex

		for _, peerID := range newPeers {
			if previous, exists := n.matchIndex[peerID]; exists {
				matchIndex[peerID] = previous
			} else {
				matchIndex[peerID] = 0
			}
		}

		n.matchIndex = matchIndex
	}

	n.configuration = configuration.Clone()
	n.peers = newPeers

	if !configuration.IsVoter(n.id) {
		n.state = StateFollower
	}

	return nil
}
