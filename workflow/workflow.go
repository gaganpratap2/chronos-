package workflow

import "errors"

var (
	ErrInvalidWorkflowID   = errors.New("workflow ID must be greater than zero")
	ErrInvalidWorkflowName = errors.New("workflow name cannot be empty")
)

type Status string

const (
	StatusPending   Status = "Pending"
	StatusRunning   Status = "Running"
	StatusCompleted Status = "Completed"
	StatusFailed    Status = "Failed"
)

type Workflow struct {
	ID     int64
	Name   string
	Status Status
}

func NewWorkflow(id int64, name string) (Workflow, error) {
	if id <= 0 {
		return Workflow{}, ErrInvalidWorkflowID
	}

	if name == "" {
		return Workflow{}, ErrInvalidWorkflowName
	}

	return Workflow{
		ID:     id,
		Name:   name,
		Status: StatusPending,
	}, nil
}
