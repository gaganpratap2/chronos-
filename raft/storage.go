package raft

import (
	"errors"
	"sync"
)

var ErrInvalidHardState = errors.New("invalid raft hard state")
var ErrInvalidLogState = errors.New("invalid raft log state")
var ErrInvalidStorage = errors.New("raft storage cannot be nil")

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

// MemoryStorage is an in-memory implementation of Storage.
// It is useful for learning and testing, but does not survive process restarts.
type MemoryStorage struct {
	mu       sync.Mutex
	state    HardState
	snapshot Snapshot
	logState LogState
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
	}

	return nil
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
		cloned.Entries[i] = LogEntry{
			Term:    entry.Term,
			Command: append([]byte(nil), entry.Command...),
		}
	}

	return cloned
}
