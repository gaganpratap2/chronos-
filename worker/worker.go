package worker

import (
	"context"
	"errors"
	"fmt"
	// "log"
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
	WorkerID          string
	Pool              *pgxpool.Pool
	Handler           Handler
	PollInterval      time.Duration
	RetryPolicy       job.RetryPolicy
	HeartbeatInterval time.Duration
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

	heartbeatCtx, heartbeatCancel := context.WithCancel(ctx)
	heartbeatErrCh := make(chan error, 1)

	go func() {
		heartbeatErr := w.heartbeat(
			heartbeatCtx,
			j.ID,
			j.FencingToken,
		)

		if heartbeatErr != nil {
			heartbeatCancel()
		}

		heartbeatErrCh <- heartbeatErr
	}()

	// Execute the handler exactly once, using the heartbeat context.
	executionErr := w.Handler.Execute(heartbeatCtx, j)

	// Stop the heartbeat and wait for it to exit.
	heartbeatCancel()
	heartbeatErr := <-heartbeatErrCh

	// If the heartbeat failed, do not write a job status
	// using ownership that may no longer be valid.
	if heartbeatErr != nil {
		return true, fmt.Errorf(
			"job %d heartbeat failed; ownership could not be confirmed: %w",
			j.ID,
			heartbeatErr,
		)
	}

	if ctx.Err() != nil {
		return true, ctx.Err()
	}

	// Handle execution failure using the existing retry logic.
	if executionErr != nil {
		_, statusErr := database.TransitionJobStatus(
			ctx,
			w.Pool,
			j.ID,
			"Failed",
			j.FencingToken,
		)
		if statusErr != nil {
			return true, fmt.Errorf(
				"job %d execution failed: %v; recording failure: %w",
				j.ID,
				executionErr,
				statusErr,
			)
		}

		_, retryErr := database.RetryJob(
			ctx,
			w.Pool,
			j.ID,
			w.RetryPolicy,
			j.FencingToken,
		)
		if retryErr != nil {
			if errors.Is(retryErr, job.ErrRetriesExhausted) {
				return true, fmt.Errorf(
					"job %d failed and has exhausted retries: %w",
					j.ID,
					retryErr,
				)
			}

			return true, fmt.Errorf(
				"job %d failed; retry scheduling failed: %w",
				j.ID,
				retryErr,
			)
		}

		return true, nil
	}

	// The handler succeeded. Mark the job completed.
	_, err = database.TransitionJobStatus(
		ctx,
		w.Pool,
		j.ID,
		"Completed",
		j.FencingToken,
	)
	if err != nil {
		return true, fmt.Errorf(
			"mark job %d completed: %w",
			j.ID,
			err,
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

func (w *Worker) heartbeat(
	ctx context.Context,
	jobID int,
	fencingToken int64,
) error {
	interval := w.HeartbeatInterval
	if interval <= 0 {
		interval = 15 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil

		case <-ticker.C:
			// Cancellation may race with the ticker firing.
			if ctx.Err() != nil {
				return nil
			}

			renewCtx, cancel := context.WithCancel(ctx)

			// Check once more before touching the database.
			if renewCtx.Err() != nil {
				cancel()
				return nil
			}

			err := database.RenewJobLease(
				renewCtx,
				w.Pool,
				jobID,
				fencingToken,
			)

			cancel()

			if err != nil {
				if ctx.Err() != nil {
					return nil
				}

				return fmt.Errorf(
					"heartbeat failed for job %d: %w",
					jobID,
					err,
				)
			}
		}
	}
}
