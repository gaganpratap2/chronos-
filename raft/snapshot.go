package raft

import (
	"errors"
)

type Snapshot struct {
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

type SnapshotStateMachine interface {
	StateMachine
	Snapshot() ([]byte, error)
}

var ErrInvalidSnapshot = errors.New("invalid raft snapshot")

func validateSnapshot(snapshot Snapshot) error {
	if snapshot.LastIncludedIndex == 0 && snapshot.LastIncludedTerm != 0 {
		return ErrInvalidSnapshot
	}

	if snapshot.LastIncludedIndex > 0 && snapshot.LastIncludedTerm == 0 {
		return ErrInvalidSnapshot
	}

	return nil
}

func cloneSnapshot(snapshot Snapshot) Snapshot {
	return Snapshot{
		LastIncludedIndex: snapshot.LastIncludedIndex,
		LastIncludedTerm:  snapshot.LastIncludedTerm,
		Data:              append([]byte(nil), snapshot.Data...),
	}
}
