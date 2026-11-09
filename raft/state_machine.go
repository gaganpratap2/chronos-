package raft

// StateMachine applies committed Raft log entries.
//
// Implementations should use the entry index as an idempotency key.
// If an application operation succeeds but returns an error, retrying
// that index must not apply its side effects twice.
type StateMachine interface {
	Apply(index uint64, command []byte) error
}
