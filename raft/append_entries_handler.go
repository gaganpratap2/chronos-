package raft

import "bytes"

// AppendEntries handles log replication and leader heartbeats.
func (n *Node) AppendEntries(
	req AppendEntriesRequest,
) (AppendEntriesResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

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

		if index > uint64(len(n.log)) {
			break
		}

		existing := n.log[index-1]

		if existing.Term != incoming.Term {
			// Never overwrite an entry already committed.
			if index <= n.commitIndex {
				return response, nil
			}

			n.log = n.log[:index-1]
			break
		}
	}

	// Append entries that are not already present.
	for i, incoming := range req.Entries {
		index := req.PrevLogIndex + uint64(i) + 1

		if index > uint64(len(n.log)) {
			n.log = append(n.log, LogEntry{
				Term:    incoming.Term,
				Command: bytes.Clone(incoming.Command),
			})
		}
	}

	// Keep existing log metadata synchronized.
	n.lastLogIndex = uint64(len(n.log))
	n.lastLogTerm = 0

	if len(n.log) > 0 {
		n.lastLogTerm = n.log[len(n.log)-1].Term
	}

	// Advance commit index, but never beyond our own log.
	if req.LeaderCommit > n.commitIndex {
		newCommitIndex := req.LeaderCommit

		if newCommitIndex > uint64(len(n.log)) {
			newCommitIndex = uint64(len(n.log))
		}

		n.commitIndex = newCommitIndex
	}

	response.Success = true
	return response, nil
}
