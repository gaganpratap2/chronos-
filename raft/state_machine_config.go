package raft

// SetStateMachine registers the state machine and applies
// any entries that were already committed.
func (n *Node) SetStateMachine(
	stateMachine StateMachine,
) error {
	if stateMachine == nil {
		return ErrNilStateMachine
	}

	n.mu.Lock()
	n.stateMachine = stateMachine
	n.mu.Unlock()

	return n.applyCommittedEntriesIfConfigured()
}

// applyCommittedEntriesIfConfigured applies committed entries
// when a state machine has been registered.
func (n *Node) applyCommittedEntriesIfConfigured() error {
	n.mu.RLock()
	stateMachine := n.stateMachine
	n.mu.RUnlock()

	if stateMachine == nil {
		return nil
	}

	return n.ApplyCommittedEntries(stateMachine)
}
