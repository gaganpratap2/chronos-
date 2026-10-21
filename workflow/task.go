package workflow

import (
	"errors"
)

var (
	ErrInvalidTaskID = errors.New("task ID must be greater than zero")
	// ErrInvalidWorkflowID = errors.New("workflow ID must be greater than zero")
	ErrInvalidTaskName = errors.New("task name cannot be empty")
)

type TaskStatus string

const (
	TaskPending   TaskStatus = "Pending"
	TaskReady     TaskStatus = "Ready"
	TaskRunning   TaskStatus = "Running"
	TaskCompleted TaskStatus = "Completed"
	TaskFailed    TaskStatus = "Failed"
	TaskBlocked   TaskStatus = "Blocked"
)

type Task struct {
	ID         int64
	WorkflowID int64
	Name       string
	Status     TaskStatus
}

func NewTask(id int64, workflowID int64, name string) (Task, error) {
	if id <= 0 {
		return Task{}, ErrInvalidTaskID
	}

	if workflowID <= 0 {
		return Task{}, ErrInvalidWorkflowID
	}

	if name == "" {
		return Task{}, ErrInvalidTaskName
	}

	return Task{
		ID:         id,
		WorkflowID: workflowID,
		Name:       name,
		Status:     TaskPending,
	}, nil
}
