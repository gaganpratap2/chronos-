package raft

import "bytes"

// AppendEntries handles log replication and leader heartbeats.
// Committed entries are applied after releasing the Raft mutex.
func (n *Node) AppendEntries(
	req AppendEntriesRequest,
) (AppendEntriesResponse, error) {
	n.mu.Lock()

	response, err := n.appendEntriesLocked(req)

	n.mu.Unlock()

	if err != nil {
		return response, err
	}

	if response.Success {
		if err := n.applyCommittedEntriesIfConfigured(); err != nil {
			return response, err
		}
	}

	return response, nil
}

func (n *Node) appendEntriesLocked(
	req AppendEntriesRequest,
) (AppendEntriesResponse, error) {
	response := AppendEntriesResponse{
		Term:    n.currentTerm,
		Success: false,
	}

	// Reject invalid or outdated requests.
	if req.Term == 0 || req.LeaderID == "" {
		return response, nil
	}

	if req.Term < n.currentTerm {
		return response, nil
	}

	// A higher-term leader forces this node to become a follower.
	if req.Term > n.currentTerm {
		hardState := HardState{
			CurrentTerm: req.Term,
			VotedFor:    "",
		}

		if err := n.storage.SaveHardState(hardState); err != nil {
			return response, err
		}

		n.currentTerm = req.Term
		n.votedFor = ""
	}

	n.state = StateFollower
	response.Term = n.currentTerm

	// Verify the previous log position.
	prevTerm, exists := n.termAt(req.PrevLogIndex)
	if !exists || prevTerm != req.PrevLogTerm {
		return response, nil
	}

	// Validate incoming entries before modifying the log.
	for _, entry := range req.Entries {
		if entry.Term == 0 || entry.Term > req.Term {
			return response, nil
		}
	}

	// Find the first conflicting entry.
	for i, incoming := range req.Entries {
		index := req.PrevLogIndex + uint64(i) + 1

		if index > n.lastLogIndexLocked() {
			break
		}

		existing, exists := n.logEntryAt(index)
		if !exists {
			return response, nil
		}

		if existing.Term != incoming.Term {
			// Never overwrite an entry already committed.
			if index <= n.commitIndex {
				return response, nil
			}

			// Convert the absolute index into a slice offset.
			offset := index - n.snapshotIndex - 1
			n.log = n.log[:offset]
			break
		}
	}

	// Append entries that are not already present.
	for i, incoming := range req.Entries {
		index := req.PrevLogIndex + uint64(i) + 1

		if index > n.lastLogIndexLocked() {
			n.log = append(n.log, LogEntry{
				Term:    incoming.Term,
				Command: bytes.Clone(incoming.Command),
			})
		}
	}

	// Synchronize absolute log metadata.
	n.lastLogIndex = n.lastLogIndexLocked()
	n.lastLogTerm = n.snapshotTerm

	if len(n.log) > 0 {
		n.lastLogTerm = n.log[len(n.log)-1].Term
	}

	// Advance commit index, but never beyond our own log.
	if req.LeaderCommit > n.commitIndex {
		newCommitIndex := req.LeaderCommit

		if newCommitIndex > n.lastLogIndex {
			newCommitIndex = n.lastLogIndex
		}

		n.commitIndex = newCommitIndex
	}

	response.Success = true
	return response, nil
}
