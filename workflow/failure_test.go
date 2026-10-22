package workflow

import "testing"

func TestPropagateFailureLinear(t *testing.T) {
	tasks := []Task{
		{
			ID:         1,
			WorkflowID: 100,
			Name:       "A",
			Status:     TaskFailed,
		},
		{
			ID:         2,
			WorkflowID: 100,
			Name:       "B",
			Status:     TaskPending,
		},
		{
			ID:         3,
			WorkflowID: 100,
			Name:       "C",
			Status:     TaskPending,
		},
	}

	dependencies := []Dependency{
		{TaskID: 2, DependsOnTaskID: 1},
		{TaskID: 3, DependsOnTaskID: 2},
	}

	result := PropagateFailure(tasks, dependencies)

	if result[1].Status != TaskBlocked {
		t.Fatalf(
			"expected task 2 to be Blocked, got %s",
			result[1].Status,
		)
	}

	if result[2].Status != TaskBlocked {
		t.Fatalf(
			"expected task 3 to be Blocked, got %s",
			result[2].Status,
		)
	}
}

func TestPropagateFailureBranching(t *testing.T) {
	tasks := []Task{
		{
			ID:         1,
			WorkflowID: 100,
			Name:       "A",
			Status:     TaskFailed,
		},
		{
			ID:         2,
			WorkflowID: 100,
			Name:       "B",
			Status:     TaskPending,
		},
		{
			ID:         3,
			WorkflowID: 100,
			Name:       "C",
			Status:     TaskPending,
		},
	}

	dependencies := []Dependency{
		{TaskID: 2, DependsOnTaskID: 1},
		{TaskID: 3, DependsOnTaskID: 1},
	}

	result := PropagateFailure(tasks, dependencies)

	if result[1].Status != TaskBlocked {
		t.Fatalf("expected B to be Blocked")
	}

	if result[2].Status != TaskBlocked {
		t.Fatalf("expected C to be Blocked")
	}
}

func TestPropagateFailureFanIn(t *testing.T) {
	tasks := []Task{
		{
			ID:         1,
			WorkflowID: 100,
			Name:       "A",
			Status:     TaskCompleted,
		},
		{
			ID:         2,
			WorkflowID: 100,
			Name:       "B",
			Status:     TaskFailed,
		},
		{
			ID:         3,
			WorkflowID: 100,
			Name:       "C",
			Status:     TaskPending,
		},
	}

	dependencies := []Dependency{
		{TaskID: 3, DependsOnTaskID: 1},
		{TaskID: 3, DependsOnTaskID: 2},
	}

	result := PropagateFailure(tasks, dependencies)

	if result[2].Status != TaskBlocked {
		t.Fatalf(
			"expected C to be Blocked, got %s",
			result[2].Status,
		)
	}
}

func TestPropagateFailureDoesNotBlockIndependentTask(t *testing.T) {
	tasks := []Task{
		{
			ID:         1,
			WorkflowID: 100,
			Name:       "A",
			Status:     TaskFailed,
		},
		{
			ID:         2,
			WorkflowID: 100,
			Name:       "B",
			Status:     TaskPending,
		},
	}

	dependencies := []Dependency{}

	result := PropagateFailure(tasks, dependencies)

	if result[1].Status != TaskPending {
		t.Fatalf(
			"expected independent task to remain Pending, got %s",
			result[1].Status,
		)
	}
}
