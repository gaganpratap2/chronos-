package raft

import "testing"

func TestStateString(t *testing.T) {
	tests := []struct {
		name  string
		state State
		want  string
	}{
		{
			name:  "follower",
			state: StateFollower,
			want:  "follower",
		},
		{
			name:  "candidate",
			state: StateCandidate,
			want:  "candidate",
		},
		{
			name:  "leader",
			state: StateLeader,
			want:  "leader",
		},
		{
			name:  "unknown",
			state: State(99),
			want:  "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.state.String()

			if got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}
