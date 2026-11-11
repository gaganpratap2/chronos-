package raft

import (
	"errors"
	"testing"
)

func TestValidateLogStateConfigurationEntries(t *testing.T) {
	validConfiguration := &ConfigurationState{
		OldVoters: []string{"A", "B", "C"},
	}

	jointConfiguration := &ConfigurationState{
		OldVoters: []string{"A", "B", "C"},
		NewVoters: []string{"A", "B", "D"},
	}

	tests := []struct {
		name    string
		entry   LogEntry
		wantErr bool
	}{
		{
			name: "legacy command without type",
			entry: LogEntry{
				Term:    1,
				Command: []byte("create-job"),
			},
		},
		{
			name: "explicit command",
			entry: LogEntry{
				Term:    1,
				Type:    LogEntryCommand,
				Command: []byte("create-job"),
			},
		},
		{
			name: "valid configuration",
			entry: LogEntry{
				Term:          1,
				Type:          LogEntryConfiguration,
				Configuration: validConfiguration,
			},
		},
		{
			name: "valid joint configuration",
			entry: LogEntry{
				Term:          1,
				Type:          LogEntryConfiguration,
				Configuration: jointConfiguration,
			},
		},
		{
			name: "configuration missing payload",
			entry: LogEntry{
				Term: 1,
				Type: LogEntryConfiguration,
			},
			wantErr: true,
		},
		{
			name: "invalid configuration",
			entry: LogEntry{
				Term: 1,
				Type: LogEntryConfiguration,
				Configuration: &ConfigurationState{
					OldVoters: []string{"A", "A"},
				},
			},
			wantErr: true,
		},
		{
			name: "command with configuration payload",
			entry: LogEntry{
				Term:          1,
				Type:          LogEntryCommand,
				Command:       []byte("create-job"),
				Configuration: validConfiguration,
			},
			wantErr: true,
		},
		{
			name: "configuration with command payload",
			entry: LogEntry{
				Term:          1,
				Type:          LogEntryConfiguration,
				Command:       []byte("create-job"),
				Configuration: validConfiguration,
			},
			wantErr: true,
		},
		{
			name: "unknown entry type",
			entry: LogEntry{
				Term: 1,
				Type: LogEntryType("unknown"),
			},
			wantErr: true,
		},
		{
			name: "zero term",
			entry: LogEntry{
				Type:    LogEntryCommand,
				Command: []byte("create-job"),
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateLogState(LogState{
				FirstIndex: 1,
				Entries:    []LogEntry{tt.entry},
			})

			if tt.wantErr && !errors.Is(err, ErrInvalidLogState) {
				t.Fatalf("validateLogState() error = %v, want %v", err, ErrInvalidLogState)
			}

			if !tt.wantErr && err != nil {
				t.Fatalf("validateLogState() unexpected error = %v", err)
			}
		})
	}
}

func TestCloneLogEntryDeepCopiesConfiguration(t *testing.T) {
	original := LogEntry{
		Term: 1,
		Type: LogEntryConfiguration,
		Configuration: &ConfigurationState{
			OldVoters: []string{"A", "B", "C"},
			NewVoters: []string{"A", "B", "D"},
		},
	}

	cloned := cloneLogEntry(original)

	cloned.Configuration.OldVoters[0] = "changed"
	cloned.Configuration.NewVoters[0] = "changed"

	if original.Configuration.OldVoters[0] != "A" {
		t.Fatal("clone modified original old voters")
	}

	if original.Configuration.NewVoters[0] != "A" {
		t.Fatal("clone modified original new voters")
	}
}

func TestCloneLogEntryDeepCopiesCommand(t *testing.T) {
	original := LogEntry{
		Term:    1,
		Command: []byte("create-job"),
	}

	cloned := cloneLogEntry(original)
	cloned.Command[0] = 'X'

	if string(original.Command) != "create-job" {
		t.Fatalf("clone modified original command: %q", original.Command)
	}
}

func TestCloneLogStatePreservesConfigurationEntry(t *testing.T) {
	original := LogState{
		FirstIndex: 1,
		Entries: []LogEntry{
			{
				Term: 1,
				Type: LogEntryConfiguration,
				Configuration: &ConfigurationState{
					OldVoters: []string{"A", "B", "C"},
					NewVoters: []string{"A", "B", "D"},
				},
			},
		},
	}

	cloned := cloneLogState(original)

	if cloned.Entries[0].Type != LogEntryConfiguration {
		t.Fatal("configuration entry type was not preserved")
	}

	if cloned.Entries[0].Configuration == nil {
		t.Fatal("configuration payload was lost")
	}

	cloned.Entries[0].Configuration.NewVoters[0] = "changed"

	if original.Entries[0].Configuration.NewVoters[0] != "A" {
		t.Fatal("cloned log state modified original configuration")
	}
}
