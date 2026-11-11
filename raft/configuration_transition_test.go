package raft

import "testing"

func TestValidateConfigurationTransition(t *testing.T) {
	stableA := ConfigurationState{
		OldVoters: []string{"A", "B", "C"},
	}

	jointAB := ConfigurationState{
		OldVoters: []string{"A", "B", "C"},
		NewVoters: []string{"A", "B", "D"},
	}

	stableB := ConfigurationState{
		OldVoters: []string{"A", "B", "D"},
	}

	stableC := ConfigurationState{
		OldVoters: []string{"A", "C", "D"},
	}

	tests := []struct {
		name    string
		current ConfigurationState
		next    ConfigurationState
		wantErr bool
	}{
		{
			name:    "stable to joint",
			current: stableA,
			next:    jointAB,
		},
		{
			name:    "stable directly to stable",
			current: stableA,
			next:    stableB,
			wantErr: true,
		},
		{
			name:    "joint to intended stable configuration",
			current: jointAB,
			next:    stableB,
		},
		{
			name:    "joint directly to another joint configuration",
			current: jointAB,
			next: ConfigurationState{
				OldVoters: []string{"A", "B", "C"},
				NewVoters: []string{"A", "C", "D"},
			},
			wantErr: true,
		},
		{
			name:    "joint to wrong stable configuration",
			current: jointAB,
			next:    stableC,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateConfigurationTransition(
				tt.current,
				tt.next,
			)

			if (err != nil) != tt.wantErr {
				t.Fatalf(
					"ValidateConfigurationTransition() error = %v, wantErr %v",
					err,
					tt.wantErr,
				)
			}
		})
	}
}
