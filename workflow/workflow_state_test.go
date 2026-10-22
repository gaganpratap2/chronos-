package workflow

import "testing"

func TestWorkflowValidTransitions(t *testing.T) {
	w := Workflow{
		ID:     1,
		Name:   "payment-workflow",
		Status: StatusPending,
	}

	if err := w.TransitionTo(StatusRunning); err != nil {
		t.Fatalf(
			"expected Pending -> Running to succeed: %v",
			err,
		)
	}

	if w.Status != StatusRunning {
		t.Fatalf(
			"expected Running, got %s",
			w.Status,
		)
	}

	if err := w.TransitionTo(StatusCompleted); err != nil {
		t.Fatalf(
			"expected Running -> Completed to succeed: %v",
			err,
		)
	}

	if w.Status != StatusCompleted {
		t.Fatalf(
			"expected Completed, got %s",
			w.Status,
		)
	}
}

func TestWorkflowRunningToFailed(t *testing.T) {
	w := Workflow{
		ID:     1,
		Name:   "payment-workflow",
		Status: StatusRunning,
	}

	if err := w.TransitionTo(StatusFailed); err != nil {
		t.Fatalf(
			"expected Running -> Failed to succeed: %v",
			err,
		)
	}

	if w.Status != StatusFailed {
		t.Fatalf(
			"expected Failed, got %s",
			w.Status,
		)
	}
}

func TestWorkflowInvalidTransitions(t *testing.T) {
	tests := []struct {
		name string
		from Status
		to   Status
	}{
		{
			name: "pending to completed",
			from: StatusPending,
			to:   StatusCompleted,
		},
		{
			name: "pending to failed",
			from: StatusPending,
			to:   StatusFailed,
		},
		{
			name: "completed to running",
			from: StatusCompleted,
			to:   StatusRunning,
		},
		{
			name: "completed to pending",
			from: StatusCompleted,
			to:   StatusPending,
		},
		{
			name: "failed to running",
			from: StatusFailed,
			to:   StatusRunning,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := Workflow{
				ID:     1,
				Name:   "test",
				Status: tt.from,
			}

			if err := w.TransitionTo(tt.to); err != ErrInvalidWorkflowTransition {
				t.Fatalf(
					"expected ErrInvalidWorkflowTransition, got %v",
					err,
				)
			}

			if w.Status != tt.from {
				t.Fatalf(
					"expected status to remain %s, got %s",
					tt.from,
					w.Status,
				)
			}
		})
	}
}
