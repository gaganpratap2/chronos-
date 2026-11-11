package raft

import (
	"errors"
	"fmt"
	"sync"
)

var (
	ErrInvalidNodeID = errors.New("raft node ID cannot be empty")
	ErrInvalidPeers  = errors.New("raft peers contain an empty or duplicate ID")
)

// Node represents one participant in a Raft cluster.
type Node struct {
	mu sync.RWMutex

	id            string
	state         State
	currentTerm   uint64
	votedFor      string
	peers         []string
	storage       Storage
	lastLogIndex  uint64
	lastLogTerm   uint64
	log           []LogEntry
	commitIndex   uint64
	nextIndex     map[string]uint64
	matchIndex    map[string]uint64
	applyMu       sync.Mutex
	lastApplied   uint64
	stateMachine  StateMachine
	snapshot      Snapshot
	snapshotIndex uint64
	snapshotTerm  uint64
	configuration ConfigurationState
}

// NewNode creates a new Raft node.
// NewNode creates a Raft node with in-memory storage.
func NewNode(id string, peers []string) (*Node, error) {
	return NewNodeWithStorage(id, peers, NewMemoryStorage())
}

func NewNodeWithStorage(id string, peers []string, storage Storage) (*Node, error) {

	if id == "" {
		return nil, ErrInvalidNodeID
	}

	if storage == nil {
		return nil, ErrInvalidStorage
	}

	seen := make(map[string]struct{}, len(peers))

	for _, peerID := range peers {
		if peerID == "" || peerID == id {
			return nil, ErrInvalidPeers
		}

		if _, exists := seen[peerID]; exists {
			return nil, ErrInvalidPeers
		}

		seen[peerID] = struct{}{}
	}

	hardState, err := storage.LoadHardState()
	if err != nil {
		return nil, err
	}

	if err := validateHardState(hardState); err != nil {
		return nil, err
	}

	initialConfiguration := ConfigurationState{
		OldVoters: append([]string{id}, peers...),
	}

	configuration := initialConfiguration.Clone()
	hasPersistedConfiguration := false

	if configurationStorage, ok := storage.(ConfigurationStorage); ok {
		persistedConfiguration, loadErr := configurationStorage.LoadConfiguration()

		switch {
		case loadErr == nil:
			if err := persistedConfiguration.Validate(); err != nil {
				return nil, err
			}

			configuration = persistedConfiguration.Clone()
			hasPersistedConfiguration = true

		case errors.Is(loadErr, ErrConfigurationNotFound):
			if err := configurationStorage.SaveConfiguration(
				initialConfiguration,
			); err != nil {
				return nil, err
			}

		default:
			return nil, loadErr
		}
	}

	if hardState.VotedFor != "" && !hasPersistedConfiguration {
		validVoter := false

		for _, voter := range configuration.OldVoters {
			if voter == hardState.VotedFor {
				validVoter = true
				break
			}
		}

		if !validVoter {
			return nil, ErrInvalidHardState
		}
	}

	peerCopy := configurationPeerIDs(id, configuration)

	node := &Node{
		id:            id,
		state:         StateFollower,
		currentTerm:   hardState.CurrentTerm,
		votedFor:      hardState.VotedFor,
		peers:         peerCopy,
		storage:       storage,
		configuration: configuration,
	}

	// Load snapshot metadata when the storage supports snapshots.
	if snapshotStorage, ok := storage.(SnapshotStorage); ok {
		snapshot, err := snapshotStorage.LoadSnapshot()
		if err != nil {
			return nil, err
		}

		if err := validateSnapshot(snapshot); err != nil {
			return nil, err
		}

		node.snapshot = cloneSnapshot(snapshot)
		node.snapshotIndex = snapshot.LastIncludedIndex
		node.snapshotTerm = snapshot.LastIncludedTerm

		// A persisted snapshot represents successfully applied entries.
		node.lastApplied = snapshot.LastIncludedIndex
		node.commitIndex = snapshot.LastIncludedIndex

		node.lastLogIndex = snapshot.LastIncludedIndex
		node.lastLogTerm = snapshot.LastIncludedTerm
	}

	// Restore retained log entries when storage supports log persistence.
	// if logStorage, ok := storage.(LogStorage); ok {
	// 	logState, err := logStorage.LoadLog()
	// 	if err != nil {
	// 		return nil, err
	// 	}

	// 	if err := validateLogState(logState); err != nil {
	// 		return nil, err
	// 	}

	// 	// A retained log must begin immediately after the snapshot.
	// 	if len(logState.Entries) > 0 {
	// 		if node.snapshotIndex == ^uint64(0) {
	// 			return nil, ErrInvalidLogState
	// 		}

	// 		expectedFirstIndex := node.snapshotIndex + 1

	// 		if logState.FirstIndex != expectedFirstIndex {
	// 			return nil, fmt.Errorf(
	// 				"%w: first index is %d, expected %d",
	// 				ErrInvalidLogState,
	// 				logState.FirstIndex,
	// 				expectedFirstIndex,
	// 			)
	// 		}

	// 		entryCount := uint64(len(logState.Entries))

	// 		if entryCount > ^uint64(0)-node.snapshotIndex {
	// 			return nil, ErrInvalidLogState
	// 		}

	// 		node.log = cloneLogState(logState).Entries

	// 		node.lastLogIndex = node.snapshotIndex + entryCount
	// 		node.lastLogTerm = node.log[len(node.log)-1].Term
	// 	}
	// }

	// Restore retained log entries when storage supports log persistence.
	if logStorage, ok := storage.(LogStorage); ok {
		logState, err := logStorage.LoadLog()
		if err != nil {
			return nil, err
		}

		if err := validateLogState(logState); err != nil {
			return nil, err
		}

		if len(logState.Entries) > 0 {
			firstIndex := logState.FirstIndex
			entryCount := uint64(len(logState.Entries))

			// Protect the absolute log-index calculation from overflow.
			if entryCount-1 > ^uint64(0)-firstIndex {
				return nil, ErrInvalidLogState
			}

			lastIndex := firstIndex + entryCount - 1

			// The log must either begin immediately after the snapshot
			// or overlap it. A gap means entries are missing.
			if node.snapshotIndex == ^uint64(0) {
				return nil, ErrInvalidLogState
			}

			expectedFirstIndex := node.snapshotIndex + 1

			if firstIndex > expectedFirstIndex {
				return nil, fmt.Errorf(
					"%w: log begins at %d but snapshot requires %d",
					ErrInvalidLogState,
					firstIndex,
					expectedFirstIndex,
				)
			}

			entries := cloneLogState(logState).Entries

			if firstIndex <= node.snapshotIndex {
				// The persisted log may contain entries already
				// included in a newer, successfully persisted snapshot.
				if node.snapshotIndex <= lastIndex {
					offset := node.snapshotIndex - firstIndex

					// The snapshot and old log must agree at their
					// shared boundary before we retain the suffix.
					if entries[offset].Term != node.snapshotTerm {
						return nil, fmt.Errorf(
							"%w: snapshot term does not match log at index %d",
							ErrInvalidLogState,
							node.snapshotIndex,
						)
					}

					entries = entries[offset+1:]
				} else {
					// The snapshot is newer than the entire old log.
					// All old entries are already covered.
					entries = nil
				}
			}

			node.log = entries
			node.lastLogIndex = node.lastLogIndexLocked()

			if len(node.log) > 0 {
				node.lastLogTerm = node.log[len(node.log)-1].Term
			}
		}
	}

	// Restore the authoritative membership from the snapshot.
	// A snapshot includes all configuration changes through its index.
	if node.snapshotIndex > 0 {
		node.configuration = node.snapshot.Configuration.Clone()
	}

	// Recover configurations applied after the snapshot.
	if configurationStorage, ok := storage.(IndexedConfigurationStorage); ok {
		records, err := configurationStorage.LoadConfigurationRecords()
		if err != nil {
			return nil, fmt.Errorf(
				"load indexed configurations: %w",
				err,
			)
		}

		current := node.configuration.Clone()
		previousIndex := uint64(0)

		for _, record := range records {
			if record.Index == 0 || record.Index <= previousIndex {
				return nil, ErrInvalidConfigurationIndex
			}
			previousIndex = record.Index

			if err := record.Configuration.Validate(); err != nil {
				return nil, err
			}

			// The snapshot already represents this configuration history.
			if record.Index <= node.snapshotIndex {
				if record.Index == node.snapshotIndex &&
					(!equalVoterSets(
						record.Configuration.OldVoters,
						current.OldVoters,
					) || !equalVoterSets(
						record.Configuration.NewVoters,
						current.NewVoters,
					)) {
					return nil, ErrConfigurationIndexConflict
				}

				continue
			}

			// Every configuration applied after the snapshot
			// must correspond to an actual configuration log entry.
			entry, exists := node.logEntryAt(record.Index)
			if !exists ||
				entry.Type != LogEntryConfiguration ||
				entry.Configuration == nil {
				return nil, fmt.Errorf(
					"%w: no configuration log entry at index %d",
					ErrConfigurationIndexConflict,
					record.Index,
				)
			}

			if !equalVoterSets(
				entry.Configuration.OldVoters,
				record.Configuration.OldVoters,
			) || !equalVoterSets(
				entry.Configuration.NewVoters,
				record.Configuration.NewVoters,
			) {
				return nil, ErrConfigurationIndexConflict
			}

			if err := ValidateConfigurationTransition(
				current,
				record.Configuration,
			); err != nil {
				return nil, err
			}

			current = record.Configuration.Clone()
		}

		node.configuration = current
		node.peers = configurationPeerIDs(
			node.id,
			node.configuration,
		)
	}

	return node, nil
}

// ID returns the node's unique ID.
func (n *Node) ID() string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.id
}

// State returns the node's current state.
func (n *Node) State() State {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.state
}

// CurrentTerm returns the current election term.
func (n *Node) CurrentTerm() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.currentTerm
}

// VotedFor returns the candidate this node voted for.
func (n *Node) VotedFor() string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.votedFor
}

// Peers returns a copy of the cluster's peer IDs.
func (n *Node) Peers() []string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return append([]string(nil), n.peers...)
}

// ClusterSize returns the total number of nodes,
// including this node.
func (n *Node) ClusterSize() int {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return len(n.peers) + 1
}

// Majority returns the number of votes needed
// to obtain a majority.
func (n *Node) Majority() int {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return (len(n.peers)+1)/2 + 1
}

// String returns a readable description of the node.
func (n *Node) String() string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return fmt.Sprintf(
		"Node{id:%s state:%s term:%d votedFor:%s}",
		n.id,
		n.state,
		n.currentTerm,
		n.votedFor,
	)
}
