package raft

// termAt returns the term of the log entry at the given index.
//
// Raft log indexes start at 1.
// Index 0 represents the empty log position.
func (n *Node) termAt(index uint64) (uint64, bool) {
	if index == 0 {
		return 0, true
	}

	if index > uint64(len(n.log)) {
		return 0, false
	}

	return n.log[index-1].Term, true
}
