package raft

// LogEntry represents one command stored in the Raft log.
type LogEntry struct {
	Term    uint64
	Command []byte
}

// AppendEntriesRequest is sent by a leader to replicate log entries  or send a heartbeat to followers.
type AppendEntriesRequest struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

// AppendEntriesResponse is returned by a follower.
type AppendEntriesResponse struct {
	Term    uint64
	Success bool
}
