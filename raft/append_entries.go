package raft

type LogEntryType string // LogEntryType identifies the kind of operation stored in the Raft log.

const (
	LogEntryCommand       LogEntryType = "command"       // LogEntryCommand represents an application state-machine command.
	LogEntryConfiguration LogEntryType = "configuration" // LogEntryConfiguration represents a cluster membership change.
)

// LogEntry represents one command or configuration change in the Raft log.
type LogEntry struct {
	Term          uint64
	Type          LogEntryType `json:"type,omitempty"`
	Command       []byte
	Configuration *ConfigurationState `json:"configuration,omitempty"`
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
