package workflow

import "errors"

var ErrInvalidWorkflowTransition = errors.New(
	"invalid workflow state transition",
)

func (w *Workflow) TransitionTo(next Status) error {
	switch w.Status {
	case StatusPending:
		if next == StatusRunning {
			w.Status = next
			return nil
		}

	case StatusRunning:
		if next == StatusCompleted || next == StatusFailed {
			w.Status = next
			return nil
		}

	case StatusCompleted, StatusFailed:
		return ErrInvalidWorkflowTransition
	}

	return ErrInvalidWorkflowTransition
}
