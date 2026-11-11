package raft

func (n *Node) initializeReplicationProgressLocked() {
	lastLogIndex := n.lastLogIndexLocked()

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

	_, knownPeer := n.nextIndex[peerID]
	if !knownPeer {
		return false
	}

	if replicatedIndex > n.lastLogIndexLocked() {
		return false
	}

	// Never allow delayed responses to reduce confirmed progress.
	if replicatedIndex > n.matchIndex[peerID] {
		n.matchIndex[peerID] = replicatedIndex
	}

	n.nextIndex[peerID] = n.matchIndex[peerID] + 1

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

	lastLogIndex := n.lastLogIndexLocked()

	// Search backwards to find the highest index that can be committed.
	for index := lastLogIndex; index > n.commitIndex; index-- {
		entryTerm, exists := n.termAt(index)
		if !exists || entryTerm != n.currentTerm {
			continue
		}

		if n.hasReplicationQuorumLocked(index) {
			n.commitIndex = index
			return true
		}
	}

	return false
}

// hasReplicationQuorumLocked checks whether an entry has been
// replicated by a quorum of the active configuration.
//
// The caller must hold n.mu.
func (n *Node) hasReplicationQuorumLocked(index uint64) bool {
	voters := make([]string, 0)

	seen := make(map[string]struct{})

	allVoters := append(
		append([]string(nil), n.configuration.OldVoters...),
		n.configuration.NewVoters...,
	)

	for _, voter := range allVoters {
		if _, exists := seen[voter]; exists {
			continue
		}

		seen[voter] = struct{}{}

		if n.matchIndex[voter] >= index {
			voters = append(voters, voter)
		}
	}

	return n.configuration.HasQuorum(voters)
}
