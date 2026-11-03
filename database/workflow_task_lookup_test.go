package database

import (
	"errors"
	"testing"
)

func TestGetWorkflowTaskIDByJobID(t *testing.T) {
	f := newExecutionOperationFixture(t)

	taskID, err := GetWorkflowTaskIDByJobID(
		f.ctx,
		f.pool,
		f.jobID,
	)
	if err != nil {
		t.Fatalf("get workflow task ID: %v", err)
	}

	if taskID != f.taskID {
		t.Fatalf(
			"expected task ID %d, got %d",
			f.taskID,
			taskID,
		)
	}
}

func TestGetWorkflowTaskIDByJobIDNotFound(t *testing.T) {
	f := newExecutionOperationFixture(t)

	unlinkedJobID := f.jobID + 1_000_000

	taskID, err := GetWorkflowTaskIDByJobID(
		f.ctx,
		f.pool,
		unlinkedJobID,
	)

	if !errors.Is(err, ErrNoWorkflowTaskForJob) {
		t.Fatalf(
			"expected ErrNoWorkflowTaskForJob, got %v",
			err,
		)
	}

	if taskID != 0 {
		t.Fatalf(
			"expected task ID 0, got %d",
			taskID,
		)
	}
}

func TestGetWorkflowTaskIDByJobIDRejectsInvalidID(t *testing.T) {
	f := newExecutionOperationFixture(t)

	_, err := GetWorkflowTaskIDByJobID(
		f.ctx,
		f.pool,
		0,
	)

	if err == nil {
		t.Fatal("expected error for invalid job ID")
	}
}
