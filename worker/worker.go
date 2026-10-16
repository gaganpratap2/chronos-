
package worker

import (
	"context"
	"errors"
	"fmt"

	"chronos/database"
	"chronos/job"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Handler interface {
	Execute(ctx context.Context, j job.Job) error
}

type Worker struct {
	Pool    *pgxpool.Pool
	Handler Handler
}


/*
SaveJob()
   ↓
Pending
   ↓
Worker.RunOnce()
   ↓
ClaimNextJob()
   ↓
Running
   ↓
fakeHandler.Execute()
   ↓
Completed
   ↓
GetJob()
   ↓
PostgreSQL says Completed
*/

func (w *Worker) RunOnce(ctx context.Context) (bool, error) {
	if w.Pool == nil {
		return false, errors.New("worker database pool is nil")
	}
	if w.Handler == nil {
		return false, errors.New("worker handler is nil")
	}

	j, err := database.ClaimNextJob(ctx, w.Pool)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("claim job: %w", err)
	}

	if err := w.Handler.Execute(ctx, j); err != nil {
		_, statusErr := database.TransitionJobStatus(
			ctx, w.Pool, j.ID, "Failed",
		)
		if statusErr != nil {
			return true, fmt.Errorf(
				"job %d execution failed: %v; recording failure: %w",
				j.ID, err, statusErr,
			)
		}
		return true, fmt.Errorf("job %d execution failed: %w", j.ID, err)
	}

	_, err = database.TransitionJobStatus(
		ctx, w.Pool, j.ID, "Completed",
	)
	if err != nil {
		return true, fmt.Errorf("mark job %d completed: %w", j.ID, err)
	}

	return true, nil
}
