package execution

import (
	"testing"
	"time"
)

func TestRecoveryDelay(t *testing.T) {
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 0, want: 30 * time.Second},
		{attempt: 1, want: 30 * time.Second},
		{attempt: 2, want: 1 * time.Minute},
		{attempt: 3, want: 2 * time.Minute},
		{attempt: 4, want: 4 * time.Minute},
		{attempt: 5, want: 8 * time.Minute},
		{attempt: 6, want: 8 * time.Minute},
	}

	for _, tt := range tests {
		t.Run(
			time.Duration(tt.attempt).String(),
			func(t *testing.T) {
				got := RecoveryDelay(tt.attempt)

				if got != tt.want {
					t.Fatalf(
						"RecoveryDelay(%d) = %v, want %v",
						tt.attempt,
						got,
						tt.want,
					)
				}
			},
		)
	}
}
