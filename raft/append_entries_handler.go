package raft

// import "bytes"

// AppendEntries handles log replication and leader heartbeats.
// Committed entries are applied after releasing the Raft mutex.
func (n *Node) AppendEntries(
	req AppendEntriesRequest,
) (AppendEntriesResponse, error) {
	n.applyMu.Lock()
	n.mu.Lock()

	response, err := n.appendEntriesLocked(req)

	n.mu.Unlock()
	n.applyMu.Unlock()

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

	// Work on a candidate copy. Do not modify n.log yet.
	candidate := cloneLogState(LogState{
		FirstIndex: 1,
		Entries:    n.log,
	}).Entries

	// Find the first conflicting entry.
	for i, incoming := range req.Entries {
		if n.lastLogIndexLocked() == ^uint64(0) {
			return response, ErrInvalidLogState
		}

		if uint64(i) > ^uint64(0)-req.PrevLogIndex-1 {
			return response, ErrInvalidLogState
		}

		index := req.PrevLogIndex + uint64(i) + 1
		candidateLastIndex := n.snapshotIndex + uint64(len(candidate))

		if index > candidateLastIndex {
			break
		}

		offset := index - n.snapshotIndex - 1
		existing := candidate[offset]

		if existing.Term != incoming.Term {
			// Never overwrite an entry already committed.
			if index <= n.commitIndex {
				return response, nil
			}

			// Truncate only the candidate.
			candidate = candidate[:offset]
			break
		}
	}

	// Append entries that are not already present.
	for i, incoming := range req.Entries {
		if uint64(i) > ^uint64(0)-req.PrevLogIndex-1 {
			return response, ErrInvalidLogState
		}

		index := req.PrevLogIndex + uint64(i) + 1
		candidateLastIndex := n.snapshotIndex + uint64(len(candidate))

		if index > candidateLastIndex {
			candidate = append(candidate, cloneLogEntry(incoming))
		}
	}

	// The applied configuration is the base for validating the
	// remaining unapplied configuration entries.
	if n.lastApplied < n.snapshotIndex {
		return response, ErrInvalidLogState
	}

	configurationChangeFound := false

	for i, entry := range candidate {
		if uint64(i) > ^uint64(0)-n.snapshotIndex-1 {
			return response, ErrInvalidLogState
		}

		index := n.snapshotIndex + uint64(i) + 1
		if index <= n.lastApplied {
			continue
		}

		if entry.Type != LogEntryConfiguration {
			continue
		}

		// Only one configuration transition may be outstanding.
		if configurationChangeFound || entry.Configuration == nil {
			return response, nil
		}

		if err := ValidateConfigurationTransition(
			n.configuration,
			*entry.Configuration,
		); err != nil {
			return response, nil
		}

		configurationChangeFound = true
	}

	// Persist the complete candidate before publishing it in memory.
	if len(req.Entries) > 0 {
		if n.snapshotIndex == ^uint64(0) {
			return response, ErrInvalidLogState
		}

		if logStorage, ok := n.storage.(LogStorage); ok {
			logState := LogState{
				FirstIndex: n.snapshotIndex + 1,
				Entries:    candidate,
			}

			if err := logStorage.SaveLog(logState); err != nil {
				return response, err
			}
		}
	}

	// Persistence succeeded (or storage does not support LogStorage).
	// Now publish the candidate in memory.
	n.log = candidate

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
