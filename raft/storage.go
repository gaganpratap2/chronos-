package raft

import (
	"errors"
	"sync"
)

var ErrInvalidHardState = errors.New("invalid raft hard state")
var ErrInvalidLogState = errors.New("invalid raft log state")
var ErrInvalidStorage = errors.New("raft storage cannot be nil")
var ErrConfigurationStorageUnsupported = errors.New("raft storage does not support configuration persistence")

func validateHardState(state HardState) error {
	if state.CurrentTerm == 0 && state.VotedFor != "" {
		return ErrInvalidHardState
	}

	return nil
}

// HardState contains Raft information that must survive restarts.
type HardState struct {
	CurrentTerm uint64
	VotedFor    string
}

// Storage defines how Raft persistent state is stored.
type Storage interface {
	LoadHardState() (HardState, error)
	SaveHardState(state HardState) error
}

type SnapshotStorage interface {
	LoadSnapshot() (Snapshot, error)
	SaveSnapshot(snapshot Snapshot) error
}

type LogState struct {
	FirstIndex uint64
	Entries    []LogEntry
}

type LogStorage interface {
	LoadLog() (LogState, error)
	SaveLog(state LogState) error
}

// ConfigurationStorage persists the active Raft voter configuration.
type ConfigurationStorage interface {
	LoadConfiguration() (ConfigurationState, error)
	SaveConfiguration(configuration ConfigurationState) error
}

// ConfigurationRecord associates a configuration with the
// Raft log index at which it was applied.
type ConfigurationRecord struct {
	Index         uint64
	Configuration ConfigurationState
}

// IndexedConfigurationStorage persists configurations with
// their corresponding Raft log indexes.
type IndexedConfigurationStorage interface {
	LoadConfigurationRecords() ([]ConfigurationRecord, error)
	SaveConfigurationAt(
		index uint64,
		configuration ConfigurationState,
	) error
}

var (
	ErrInvalidConfigurationIndex = errors.New(
		"invalid raft configuration index",
	)

	ErrConfigurationIndexConflict = errors.New(
		"raft configuration index conflict",
	)
)

// MemoryStorage is an in-memory implementation of Storage.
// It is useful for learning and testing, but does not survive process restarts.
type MemoryStorage struct {
	mu                   sync.Mutex
	state                HardState
	snapshot             Snapshot
	logState             LogState
	configuration        ConfigurationState
	configurationRecords []ConfigurationRecord
}

// NewMemoryStorage creates an empty in-memory Raft storage.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		logState: LogState{
			FirstIndex: 1,
		},
	}
}

// LoadHardState returns the saved Raft state.
func (s *MemoryStorage) LoadHardState() (HardState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.state, nil
}

// SaveHardState saves the Raft state.
// func (s *MemoryStorage) SaveHardState(state HardState) error {
// 	s.mu.Lock()
// 	defer s.mu.Unlock()

// 	s.state = state

// 	return nil
// }

func (s *MemoryStorage) SaveHardState(state HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := validateHardState(state); err != nil {
		return err
	}

	s.state = state

	return nil
}

// returns an independent copy of the saved snapshot.
func (s *MemoryStorage) LoadSnapshot() (Snapshot, error) {

	s.mu.Lock()
	defer s.mu.Unlock()

	return cloneSnapshot(s.snapshot), nil
}

// validates and saves an independent copy of the snapshot.

func (s *MemoryStorage) SaveSnapshot(snapshot Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := validateSnapshot(snapshot); err != nil {
		return err
	}

	s.snapshot = cloneSnapshot(snapshot)
	return nil
}

func (s *MemoryStorage) LoadLog() (LogState, error) { //returns an independent copy of the retained log.
	s.mu.Lock()
	defer s.mu.Unlock()

	return cloneLogState(s.logState), nil
}

// SaveLog validates and saves an independent copy of the retained log.
func (s *MemoryStorage) SaveLog(state LogState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := validateLogState(state); err != nil {
		return err
	}

	s.logState = cloneLogState(state)

	return nil
}

func validateLogState(state LogState) error {
	if state.FirstIndex == 0 {
		return ErrInvalidLogState
	}

	for _, entry := range state.Entries {
		if entry.Term == 0 {
			return ErrInvalidLogState
		}

		switch entry.Type {
		case "", LogEntryCommand:
			// Empty Type preserves compatibility with existing log records.
			if entry.Configuration != nil {
				return ErrInvalidLogState
			}

		case LogEntryConfiguration:
			if entry.Configuration == nil {
				return ErrInvalidLogState
			}

			if err := entry.Configuration.Validate(); err != nil {
				return ErrInvalidLogState
			}

			if len(entry.Command) > 0 {
				return ErrInvalidLogState
			}

		default:
			return ErrInvalidLogState
		}
	}

	return nil
}

func cloneLogEntry(entry LogEntry) LogEntry {
	cloned := LogEntry{
		Term:    entry.Term,
		Type:    entry.Type,
		Command: append([]byte(nil), entry.Command...),
	}

	if entry.Configuration != nil {
		configuration := entry.Configuration.Clone()
		cloned.Configuration = &configuration
	}

	return cloned
}

func cloneLogState(state LogState) LogState {
	cloned := LogState{
		FirstIndex: state.FirstIndex,
	}

	if state.Entries == nil {
		return cloned
	}

	cloned.Entries = make([]LogEntry, len(state.Entries))

	for i, entry := range state.Entries {
		cloned.Entries[i] = cloneLogEntry(entry)
	}

	return cloned
}

// LoadConfiguration returns an independent copy of the saved configuration.
func (s *MemoryStorage) LoadConfiguration() (ConfigurationState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.configuration.OldVoters) == 0 {
		return ConfigurationState{}, ErrConfigurationNotFound
	}

	return s.configuration.Clone(), nil
}

// SaveConfiguration validates and saves an independent copy.
func (s *MemoryStorage) SaveConfiguration(
	configuration ConfigurationState,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := configuration.Validate(); err != nil {
		return err
	}

	s.configuration = configuration.Clone()

	return nil
}

// LoadConfigurationRecords returns independent copies of all
// indexed configuration records.
func (s *MemoryStorage) LoadConfigurationRecords() (
	[]ConfigurationRecord,
	error,
) {
	s.mu.Lock()
	defer s.mu.Unlock()

	records := make([]ConfigurationRecord, len(s.configurationRecords))

	for i, record := range s.configurationRecords {
		records[i] = ConfigurationRecord{
			Index:         record.Index,
			Configuration: record.Configuration.Clone(),
		}
	}

	return records, nil
}

// SaveConfigurationAt persists a configuration associated with
// its Raft log index.
//
// Repeating the same index and configuration is idempotent.
// Reusing an index for a different configuration is rejected.
func (s *MemoryStorage) SaveConfigurationAt(
	index uint64,
	configuration ConfigurationState,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if index == 0 {
		return ErrInvalidConfigurationIndex
	}

	if err := configuration.Validate(); err != nil {
		return err
	}

	if len(s.configurationRecords) > 0 {
		last := s.configurationRecords[len(s.configurationRecords)-1]

		if index == last.Index {
			if equalVoterSets(
				last.Configuration.OldVoters,
				configuration.OldVoters,
			) && equalVoterSets(
				last.Configuration.NewVoters,
				configuration.NewVoters,
			) {
				return nil
			}

			return ErrConfigurationIndexConflict
		}

		if index < last.Index {
			return ErrInvalidConfigurationIndex
		}
	}

	s.configurationRecords = append(
		s.configurationRecords,
		ConfigurationRecord{
			Index:         index,
			Configuration: configuration.Clone(),
		},
	)

	return nil
}
