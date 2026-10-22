package workflow

import "testing"

func TestTaskValidTransitions(t *testing.T) {
	task := Task{
		ID:         1,
		WorkflowID: 10,
		Name:       "send-email",
		Status:     TaskPending,
	}

	if err := task.TransitionTo(TaskReady); err != nil {
		t.Fatalf("expected Pending -> Ready to succeed: %v", err)
	}

	if task.Status != TaskReady {
		t.Fatalf(
			"expected task status Ready, got %s",
			task.Status,
		)
	}

	if err := task.TransitionTo(TaskRunning); err != nil {
		t.Fatalf("expected Ready -> Running to succeed: %v", err)
	}

	if task.Status != TaskRunning {
		t.Fatalf(
			"expected task status Running, got %s",
			task.Status,
		)
	}

	if err := task.TransitionTo(TaskCompleted); err != nil {
		t.Fatalf(
			"expected Running -> Completed to succeed: %v",
			err,
		)
	}

	if task.Status != TaskCompleted {
		t.Fatalf(
			"expected task status Completed, got %s",
			task.Status,
		)
	}
}

func TestTaskRunningToFailed(t *testing.T) {
	task := Task{
		ID:         1,
		WorkflowID: 10,
		Name:       "send-email",
		Status:     TaskRunning,
	}

	if err := task.TransitionTo(TaskFailed); err != nil {
		t.Fatalf(
			"expected Running -> Failed to succeed: %v",
			err,
		)
	}

	if task.Status != TaskFailed {
		t.Fatalf(
			"expected task status Failed, got %s",
			task.Status,
		)
	}
}

func TestTaskInvalidTransitions(t *testing.T) {
	tests := []struct {
		name string
		from TaskStatus
		to   TaskStatus
	}{
		{
			name: "pending to completed",
			from: TaskPending,
			to:   TaskCompleted,
		},
		{
			name: "ready to completed",
			from: TaskReady,
			to:   TaskCompleted,
		},
		{
			name: "completed to pending",
			from: TaskCompleted,
			to:   TaskPending,
		},
		{
			name: "failed to running",
			from: TaskFailed,
			to:   TaskRunning,
		},
		{
			name: "blocked to running",
			from: TaskBlocked,
			to:   TaskRunning,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := Task{
				ID:         1,
				WorkflowID: 10,
				Name:       "test",
				Status:     tt.from,
			}

			if err := task.TransitionTo(tt.to); err != ErrInvalidTaskTransition {
				t.Fatalf(
					"expected ErrInvalidTaskTransition, got %v",
					err,
				)
			}

			if task.Status != tt.from {
				t.Fatalf(
					"expected status to remain %s, got %s",
					tt.from,
					task.Status,
				)
			}
		})
	}
}