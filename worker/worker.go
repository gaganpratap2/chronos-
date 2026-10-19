package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"chronos/database"
	"chronos/job"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Handler interface {
	Execute(ctx context.Context, j job.Job) error
}

type Worker struct {
	Pool         *pgxpool.Pool
	Handler      Handler
	PollInterval time.Duration
	RetryPolicy  job.RetryPolicy
	WorkerID     string
}

// The Worker is responsible for picking up jobs and managing their execution. The Handler is responsible for actually doing the job.

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

// execute only one job

func (w *Worker) RunOnce(ctx context.Context) (bool, error) {
	if w.Pool == nil {
		return false, errors.New("worker database pool is nil")
	}
	if w.Handler == nil {
		return false, errors.New("worker handler is nil")
	}

	j, err := database.ClaimNextJob(ctx, w.Pool, w.WorkerID)
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

		_, retryErr := database.RetryJob(ctx, w.Pool, j.ID, w.RetryPolicy)
		if retryErr != nil {
			if errors.Is(retryErr, job.ErrRetriesExhausted) {
				return true, fmt.Errorf(
					"job %d failed and has exhausted retries: %w",
					j.ID, retryErr,
				)
			}

			return true, fmt.Errorf(
				"job %d failed; retry scheduling failed: %w",
				j.ID, retryErr,
			)
		}

		// return true, fmt.Errorf(
		// 	"job %d execution failed and was scheduled for retry: %w",
		// 	j.ID, err,
		// )

		return true, nil
	}

	_, err = database.TransitionJobStatus(
		ctx, w.Pool, j.ID, "Completed",
	)
	if err != nil {
		return true, fmt.Errorf(
			"mark job %d completed: %w",
			j.ID, err,
		)
	}

	return true, nil
}

/*
Run()
  ↓
RunOnce() → Job 2 → Completed
  ↓
RunOnce() → Job 3 → Completed
  ↓
RunOnce() → no job
  ↓
Run() returns nil
*/

// func (w *Worker) Run(ctx context.Context) error {
// 	for {
// 		if err := ctx.Err(); err != nil {
// 			return err
// 		}

// 		ran, err := w.RunOnce(ctx)
// 		if err != nil {
// 			return err
// 		}

// 		if !ran {
// 			return nil
// 		}
// 	}
// }

// Run continuously processes available jobs until the context is cancelled.
func (w *Worker) Run(ctx context.Context) error {

	pollInterval := w.PollInterval
	if pollInterval <= 0 {
		pollInterval = 500 * time.Millisecond
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		ran, err := w.RunOnce(ctx)
		if err != nil {
			return err
		}

		if ran {
			continue
		}

		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-time.After(pollInterval):
		}
	}
}
