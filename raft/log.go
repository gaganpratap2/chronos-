package raft

// lastLogIndexLocked returns the absolute index of the last
// retained log entry, or the snapshot index if no entries remain.
//
// The caller must hold n.mu.
func (n *Node) lastLogIndexLocked() uint64 {
	return n.snapshotIndex + uint64(len(n.log))
}

// logEntryAt returns the retained log entry at an absolute index.
//
// The caller must hold n.mu.
func (n *Node) logEntryAt(index uint64) (LogEntry, bool) {
	if index <= n.snapshotIndex {
		return LogEntry{}, false
	}

	if index > n.lastLogIndexLocked() {
		return LogEntry{}, false
	}

	offset := index - n.snapshotIndex - 1

	return n.log[offset], true
}

// termAt returns the term at an absolute log index.
//
// Raft indexes start at 1.
// Index 0 represents the empty log position.
// The snapshot boundary represents the final entry included
// in the snapshot.
func (n *Node) termAt(index uint64) (uint64, bool) {
	if index == 0 {
		return 0, true
	}

	if index == n.snapshotIndex {
		return n.snapshotTerm, true
	}

	if index < n.snapshotIndex {
		return 0, false
	}

	entry, exists := n.logEntryAt(index)
	if !exists {
		return 0, false
	}

	return entry.Term, true
}
