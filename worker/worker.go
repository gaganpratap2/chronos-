package worker

import (
	"context"
	"errors"
	"fmt"
	// "log"
	"chronos/observability"
	"time"

	"chronos/database"
	"chronos/execution"
	"chronos/job"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Handler interface {
	Execute(ctx context.Context, j job.Job) error
}

type OperationExecutionContext struct {
	WorkflowTaskID int64
	FencingToken   int64
	Operations     execution.OperationExecutor
}

type OperationAwareHandler interface {
	Handler

	ExecuteWithOperations(
		ctx context.Context,
		j job.Job,
		execContext OperationExecutionContext,
	) error
}

type Worker struct {
	WorkerID          string
	Pool              *pgxpool.Pool
	Handler           Handler
	PollInterval      time.Duration
	RetryPolicy       job.RetryPolicy
	HeartbeatInterval time.Duration
	OperationExecutor execution.OperationExecutor
	Metrics           *observability.Metrics
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
	return w.runOnce(ctx, func(claimCtx context.Context) (job.Job, error) {
		return database.ClaimNextJob(
			claimCtx,
			w.Pool,
			w.WorkerID,
		)
	})
}

// runOnce executes one claimed job.
// The claimJob function allows tests to provide a deterministic job.
func (w *Worker) runOnce(
	ctx context.Context,
	claimJob func(context.Context) (job.Job, error),
) (bool, error) {

	if w.Pool == nil {
		return false, errors.New("worker database pool is nil")
	}

	if w.Handler == nil {
		return false, errors.New("worker handler is nil")
	}

	if claimJob == nil {
		return false, errors.New("worker claim function is nil")
	}

	operationHandler, operationAware := w.Handler.(OperationAwareHandler)

	if operationAware && w.OperationExecutor == nil {
		return false, errors.New(
			"operation-aware handler requires an operation executor",
		)
	}

	// Step 1: Claim a job.
	j, err := claimJob(ctx)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}

		return false, fmt.Errorf("claim job: %w", err)
	}

	// Track the job while this worker processes it.
	if w.Metrics != nil {
		w.Metrics.JobsRunning.Inc()
		defer w.Metrics.JobsRunning.Dec()
	}

	// Step 2: Start the heartbeat.
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

	executionStarted := time.Now()

	// Step 3: Execute the handler.
	var executionErr error

	if operationAware {
		workflowTaskID, lookupErr := database.GetWorkflowTaskIDByJobID(
			heartbeatCtx,
			w.Pool,
			j.ID,
		)

		switch {
		case errors.Is(lookupErr, database.ErrNoWorkflowTaskForJob):
			// Standalone jobs do not participate in workflow-level
			// execution idempotency.
			executionErr = w.Handler.Execute(heartbeatCtx, j)

		case lookupErr != nil:
			executionErr = fmt.Errorf(
				"load workflow task for job %d: %w",
				j.ID,
				lookupErr,
			)

		default:
			executionErr = operationHandler.ExecuteWithOperations(
				heartbeatCtx,
				j,
				OperationExecutionContext{
					WorkflowTaskID: workflowTaskID,
					FencingToken:   j.FencingToken,
					Operations:     w.OperationExecutor,
				},
			)
		}
	} else {
		executionErr = w.Handler.Execute(heartbeatCtx, j)
	}

	if w.Metrics != nil {
		w.Metrics.JobExecutionDuration.Observe(
			time.Since(executionStarted).Seconds(),
		)
	}

	// Step 4: Stop the heartbeat and wait for it to exit.
	heartbeatCancel()
	heartbeatErr := <-heartbeatErrCh

	// Step 5: Verify that ownership remained valid.
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

	// Step 6: Handle execution failure.
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

		// A confirmed provider failure is terminal.
		// Do not schedule another retry for this logical operation.
		if errors.Is(executionErr, execution.ErrOperationFailed) {

			if w.Metrics != nil {
				w.Metrics.JobsFailed.Inc()
			}

			if err := database.ReconcileWorkflowForJob(
				ctx,
				w.Pool,
				int64(j.ID),
			); err != nil {
				return true, fmt.Errorf(
					"reconcile workflow after terminal job failure: %w",
					err,
				)
			}

			return true, nil
		}

		// Other execution errors remain eligible for retry.
		_, retryErr := database.RetryJob(
			ctx,
			w.Pool,
			j.ID,
			w.RetryPolicy,
			j.FencingToken,
		)

		if retryErr != nil {
			if errors.Is(retryErr, job.ErrRetriesExhausted) {

				if w.Metrics != nil {
					w.Metrics.JobsFailed.Inc()
				}

				if err := database.ReconcileWorkflowForJob(
					ctx,
					w.Pool,
					int64(j.ID),
				); err != nil {
					return true, fmt.Errorf(
						"job %d exhausted retries; workflow reconciliation failed: %w",
						j.ID,
						err,
					)
				}

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

		if w.Metrics != nil {
			w.Metrics.JobRetries.Inc()
		}

		return true, nil
	}

	// Step 7: Mark the job completed.
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

	if w.Metrics != nil {
		w.Metrics.JobsCompleted.Inc()
	}

	if err := database.ReconcileWorkflowForJob(
		ctx,
		w.Pool,
		int64(j.ID),
	); err != nil {
		return true, fmt.Errorf(
			"job %d completed but workflow reconciliation failed: %w",
			j.ID,
			err,
		)
	}

	// if w.Metrics != nil {
	// 	w.Metrics.JobRetries.Inc()
	// }

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

	if err := ctx.Err(); err != nil {
		return err
	}

	if w.Metrics != nil {
		w.Metrics.ActiveWorkers.Inc()
		defer w.Metrics.ActiveWorkers.Dec()
	}

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
