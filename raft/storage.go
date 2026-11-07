package raft

import (
	"errors"
	"sync"
)

var ErrInvalidHardState = errors.New("invalid raft hard state")
var ErrInvalidStorage = errors.New("raft storage cannot be nil")

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

// MemoryStorage is an in-memory implementation of Storage.
// It is useful for learning and testing, but does not survive process restarts.
type MemoryStorage struct {
	mu    sync.Mutex
	state HardState
}

// NewMemoryStorage creates an empty in-memory Raft storage.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{}
}

// LoadHardState returns the saved Raft state.
func (s *MemoryStorage) LoadHardState() (HardState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.state, nil
}

// SaveHardState saves the Raft state.
func (s *MemoryStorage) SaveHardState(state HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.state = state

	return nil
}
