package workflow

import "errors"

var (
	ErrInvalidDependencyTaskID = errors.New("dependency task ID must be greater than zero")
	ErrSelfDependency          = errors.New("task cannot depend on itself")
)

type Dependency struct {
	TaskID          int64
	DependsOnTaskID int64
}

func NewDependency(taskID int64, dependsOnTaskID int64) (Dependency, error) {
	if taskID <= 0 || dependsOnTaskID <= 0 {
		return Dependency{}, ErrInvalidDependencyTaskID
	}

	if taskID == dependsOnTaskID {
		return Dependency{}, ErrSelfDependency
	}

	return Dependency{
		TaskID:          taskID,
		DependsOnTaskID: dependsOnTaskID,
	}, nil
}

func ValidateDependency(task Task, dependencyTask Task) error {
	if task.WorkflowID != dependencyTask.WorkflowID {
		return errors.New("dependency tasks must belong to the same workflow")
	}

	return nil
}
