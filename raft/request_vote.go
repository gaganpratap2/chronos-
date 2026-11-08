package raft

// RequestVoteRequest is sent by a candidate
// asking another node for its vote.
type RequestVoteRequest struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

// RequestVoteResponse is returned by the receiving node.
type RequestVoteResponse struct {
	Term        uint64
	VoteGranted bool
}

// RequestVote evaluates a candidate's request for a vote.
func (n *Node) RequestVote(
	req RequestVoteRequest,
) (RequestVoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	response := RequestVoteResponse{
		Term:        n.currentTerm,
		VoteGranted: false,
	}

	// Reject malformed requests.
	if req.Term == 0 || req.CandidateID == "" {
		return response, nil
	}

	// Reject candidates that are not cluster members.
	knownPeer := false
	for _, peerID := range n.peers {
		if peerID == req.CandidateID {
			knownPeer = true
			break
		}
	}

	if !knownPeer {
		return response, nil
	}

	// Reject requests from older terms.
	if req.Term < n.currentTerm {
		return response, nil
	}

	// A higher term requires clearing our previous vote.
	if req.Term > n.currentTerm {
		hardState := HardState{
			CurrentTerm: req.Term,
			VotedFor:    "",
		}

		// Persist before updating memory.
		if err := n.storage.SaveHardState(hardState); err != nil {
			return response, err
		}

		n.currentTerm = req.Term
		n.votedFor = ""
		n.state = StateFollower
	}

	response.Term = n.currentTerm

	// Do not vote twice in the same term.
	if n.votedFor != "" && n.votedFor != req.CandidateID {
		return response, nil
	}

	// Raft log freshness comparison:
	// 1. Higher last log term wins.
	// 2. If terms match, higher or equal index wins.
	candidateLogIsUpToDate :=
		req.LastLogTerm > n.lastLogTerm ||
			(req.LastLogTerm == n.lastLogTerm &&
				req.LastLogIndex >= n.lastLogIndex)

	if !candidateLogIsUpToDate {
		return response, nil
	}

	// Persist the vote before granting it.
	hardState := HardState{
		CurrentTerm: n.currentTerm,
		VotedFor:    req.CandidateID,
	}

	if err := n.storage.SaveHardState(hardState); err != nil {
		return response, err
	}

	n.votedFor = req.CandidateID
	n.state = StateFollower

	response.VoteGranted = true
	return response, nil
}
