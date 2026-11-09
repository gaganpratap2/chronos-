package raft

func (n *Node) initializeReplicationProgressLocked() {
	lastLogIndex := uint64(len(n.log))

	n.nextIndex = make(map[string]uint64, len(n.peers))
	n.matchIndex = make(map[string]uint64, len(n.peers)+1)

	// The leader already has its own log.
	n.matchIndex[n.id] = lastLogIndex

	// Initially, assume every follower needs entries
	// starting immediately after the leader's last entry.
	for _, peerID := range n.peers {
		n.nextIndex[peerID] = lastLogIndex + 1
		n.matchIndex[peerID] = 0
	}
}

// recordReplicationSuccessLocked records confirmed follower replication.
// The caller must hold n.mu.
func (n *Node) recordReplicationSuccessLocked(
	peerID string,
	leaderTerm uint64,
	replicatedIndex uint64,
) bool {
	if n.state != StateLeader || n.currentTerm != leaderTerm {
		return false
	}

	// nextIndex, knownPeer := n.nextIndex[peerID]
	_, knownPeer := n.nextIndex[peerID]
	if !knownPeer {
		return false
	}

	if replicatedIndex > uint64(len(n.log)) {
		return false
	}

	// Never allow delayed responses to reduce confirmed progress.
	if replicatedIndex > n.matchIndex[peerID] {
		n.matchIndex[peerID] = replicatedIndex
	}

	n.nextIndex[peerID] = n.matchIndex[peerID] + 1
	// _, knownPeer = n.nextIndex[peerID]
	return true
}

// recordReplicationFailureLocked backs up the next replication position.
// The caller must hold n.mu.
func (n *Node) recordReplicationFailureLocked(
	peerID string,
	leaderTerm uint64,
	requestNextIndex uint64,
) bool {
	if n.state != StateLeader || n.currentTerm != leaderTerm {
		return false
	}

	currentNextIndex, knownPeer := n.nextIndex[peerID]
	if !knownPeer {
		return false
	}

	// Ignore failures from an outdated replication request.
	if currentNextIndex != requestNextIndex {
		return false
	}

	// Do not move behind an index already confirmed on this follower.
	minimumNextIndex := n.matchIndex[peerID] + 1

	if currentNextIndex > minimumNextIndex {
		n.nextIndex[peerID]--
	}

	return true
}

// advanceCommitIndexLocked advances the commit index when a majority
// has replicated an entry from the current term.
//
// The caller must hold n.mu.
func (n *Node) advanceCommitIndexLocked() bool {
	if n.state != StateLeader {
		return false
	}

	lastLogIndex := uint64(len(n.log))
	majority := (len(n.peers)+1)/2 + 1

	// Search backwards to find the highest index that can be committed.
	for index := lastLogIndex; index > n.commitIndex; index-- {
		entryTerm, exists := n.termAt(index)
		if !exists || entryTerm != n.currentTerm {
			continue
		}

		replicatedCount := 0

		// Count the leader and followers that replicated this entry.
		for _, peerID := range append([]string{n.id}, n.peers...) {
			if n.matchIndex[peerID] >= index {
				replicatedCount++
			}
		}

		if replicatedCount >= majority {
			n.commitIndex = index
			return true
		}
	}

	return false
}
