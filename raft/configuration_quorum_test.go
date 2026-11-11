package raft

import "testing"

func TestConfigurationHasQuorum(t *testing.T) {
	tests := []struct {
		name          string
		configuration ConfigurationState
		votes         []string
		want          bool
	}{
		{
			name: "normal configuration majority",
			configuration: ConfigurationState{
				OldVoters: []string{"A", "B", "C"},
			},
			votes: []string{"A", "B"},
			want:  true,
		},
		{
			name: "normal configuration insufficient votes",
			configuration: ConfigurationState{
				OldVoters: []string{"A", "B", "C"},
			},
			votes: []string{"A"},
			want:  false,
		},
		{
			name: "joint configuration satisfies both majorities",
			configuration: ConfigurationState{
				OldVoters: []string{"A", "B", "C"},
				NewVoters: []string{"A", "B", "D"},
			},
			votes: []string{"A", "B"},
			want:  true,
		},
		{
			name: "old majority without new majority",
			configuration: ConfigurationState{
				OldVoters: []string{"A", "B", "C"},
				NewVoters: []string{"A", "B", "D"},
			},
			votes: []string{"A", "C"},
			want:  false,
		},
		{
			name: "new majority without old majority",
			configuration: ConfigurationState{
				OldVoters: []string{"A", "B", "C"},
				NewVoters: []string{"A", "B", "D"},
			},
			votes: []string{"B", "D"},
			want:  false,
		},
		{
			name: "duplicate votes cannot create majority",
			configuration: ConfigurationState{
				OldVoters: []string{"A", "B", "C"},
			},
			votes: []string{"A", "A"},
			want:  false,
		},
		{
			name: "unknown voters do not count",
			configuration: ConfigurationState{
				OldVoters: []string{"A", "B", "C"},
			},
			votes: []string{"A", "X", "Y"},
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.configuration.HasQuorum(tt.votes)

			if got != tt.want {
				t.Errorf(
					"HasQuorum() = %v, want %v",
					got,
					tt.want,
				)
			}
		})
	}
}

func TestConfigurationHasQuorumWithDisjointJointVoterSets(t *testing.T) {
	configuration := ConfigurationState{
		OldVoters: []string{"A", "B", "C"},
		NewVoters: []string{"D", "E", "F"},
	}

	tests := []struct {
		name  string
		votes []string
		want  bool
	}{
		{
			name:  "both majorities",
			votes: []string{"A", "B", "D", "E"},
			want:  true,
		},
		{
			name:  "old majority only",
			votes: []string{"A", "B", "C"},
			want:  false,
		},
		{
			name:  "new majority only",
			votes: []string{"D", "E", "F"},
			want:  false,
		},
		{
			name:  "combined majority but missing old majority",
			votes: []string{"A", "D", "E", "F"},
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := configuration.HasQuorum(tt.votes)

			if got != tt.want {
				t.Errorf("HasQuorum() = %v, want %v", got, tt.want)
			}
		})
	}
}
