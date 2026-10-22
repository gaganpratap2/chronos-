package workflow

import "errors"

var ErrInvalidTaskTransition = errors.New("invalid task state transition")

func (t *Task) TransitionTo(next TaskStatus) error {
	switch t.Status {
	case TaskPending:
		if next == TaskReady {
			t.Status = next
			return nil
		}

	case TaskReady:
		if next == TaskRunning {
			t.Status = next
			return nil
		}

	case TaskRunning:
		if next == TaskCompleted || next == TaskFailed {
			t.Status = next
			return nil
		}

	case TaskFailed:
		return ErrInvalidTaskTransition

	case TaskCompleted:
		return ErrInvalidTaskTransition

	case TaskBlocked:
		return ErrInvalidTaskTransition
	}

	return ErrInvalidTaskTransition
}