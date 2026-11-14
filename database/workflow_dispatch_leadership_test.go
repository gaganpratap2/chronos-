package database

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/joho/godotenv"
)

func TestClaimTaskAndCreateJobAsLeaderRejectsInvalidLeadership(
	t *testing.T,
) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		30*time.Second,
	)
	defer cancel()

	if err := godotenv.Load("../.env"); err != nil {
		t.Fatalf("load .env: %v", err)
	}

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("connect database: %v", err)
	}
	defer pool.Close()

	// Do not interfere with another running scheduler.
	var leadershipAvailable bool

	err = pool.QueryRow(
		ctx,
		`
		SELECT
			instance_id IS NULL
			OR lease_until <= clock_timestamp()
		FROM scheduler_leadership
		WHERE singleton_id = 1
		`,
	).Scan(&leadershipAvailable)

	if err != nil {
		t.Fatalf("check leadership availability: %v", err)
	}

	if !leadershipAvailable {
		t.Skip("another scheduler currently owns active leadership")
	}

	instanceID := fmt.Sprintf(
		"dispatch-test-%d",
		time.Now().UnixNano(),
	)

	const leaseDuration = 30 * time.Second

	if err := RegisterSchedulerInstance(
		ctx,
		pool,
		instanceID,
		"dispatch-test-host",
		leaseDuration,
	); err != nil {
		t.Fatalf("register scheduler: %v", err)
	}

	defer func() {
		_, _ = pool.Exec(
			context.Background(),
			`
			UPDATE scheduler_leadership
			SET instance_id = NULL,
			    lease_until = NULL,
			    updated_at = NOW()
			WHERE singleton_id = 1
			  AND instance_id = $1
			`,
			instanceID,
		)

		_, _ = pool.Exec(
			context.Background(),
			`DELETE FROM scheduler_instances WHERE id = $1`,
			instanceID,
		)
	}()

	leadership, err := AcquireSchedulerLeadership(
		ctx,
		pool,
		instanceID,
		leaseDuration,
	)
	if err != nil {
		t.Fatalf("acquire leadership: %v", err)
	}

	// Case 1: A different scheduler cannot dispatch.
	_, err = ClaimTaskAndCreateJobAsLeader(
		ctx,
		pool,
		"another-scheduler",
		leadership.FencingToken,
	)

	if !errors.Is(err, ErrSchedulerLeaseLost) {
		t.Fatalf(
			"expected lease lost for different scheduler, got: %v",
			err,
		)
	}

	// Case 2: An outdated fencing token cannot dispatch.
	_, err = ClaimTaskAndCreateJobAsLeader(
		ctx,
		pool,
		instanceID,
		leadership.FencingToken+1,
	)

	if !errors.Is(err, ErrSchedulerLeaseLost) {
		t.Fatalf(
			"expected lease lost for outdated token, got: %v",
			err,
		)
	}

	// Case 3: An expired scheduler instance cannot dispatch.
	_, err = pool.Exec(
		ctx,
		`
		UPDATE scheduler_instances
		SET lease_until = clock_timestamp() - INTERVAL '1 second'
		WHERE id = $1
		`,
		instanceID,
	)
	if err != nil {
		t.Fatalf("expire scheduler instance: %v", err)
	}

	_, err = ClaimTaskAndCreateJobAsLeader(
		ctx,
		pool,
		instanceID,
		leadership.FencingToken,
	)

	if !errors.Is(err, ErrSchedulerLeaseLost) {
		t.Fatalf(
			"expected lease lost for expired instance, got: %v",
			err,
		)
	}

	// Restore the instance lease so we can test release separately.
	_, err = pool.Exec(
		ctx,
		`
		UPDATE scheduler_instances
		SET lease_until = clock_timestamp() + INTERVAL '30 seconds'
		WHERE id = $1
		`,
		instanceID,
	)
	if err != nil {
		t.Fatalf("restore scheduler instance lease: %v", err)
	}

	// Case 4: A scheduler that released leadership cannot dispatch.
	if err := ReleaseSchedulerLeadership(
		ctx,
		pool,
		instanceID,
		leadership.FencingToken,
	); err != nil {
		t.Fatalf("release leadership: %v", err)
	}

	_, err = ClaimTaskAndCreateJobAsLeader(
		ctx,
		pool,
		instanceID,
		leadership.FencingToken,
	)

	if !errors.Is(err, ErrSchedulerLeaseLost) {
		t.Fatalf(
			"expected lease lost after leadership release, got: %v",
			err,
		)
	}
}

func TestClaimTaskAndCreateJobAsValidLeader(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		20*time.Second,
	)
	defer cancel()

	if err := godotenv.Load("../.env"); err != nil {
		t.Fatalf("load .env: %v", err)
	}

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("connect database: %v", err)
	}
	defer pool.Close()

	// Avoid interfering with another running scheduler.
	var leadershipAvailable bool

	err = pool.QueryRow(
		ctx,
		`
		SELECT instance_id IS NULL
		    OR lease_until <= clock_timestamp()
		FROM scheduler_leadership
		WHERE singleton_id = 1
		`,
	).Scan(&leadershipAvailable)

	if err != nil {
		t.Fatalf("check leadership availability: %v", err)
	}

	if !leadershipAvailable {
		t.Skip("another scheduler currently owns active leadership")
	}

	workflowID := time.Now().UnixNano()
	taskID := workflowID + 1

	instanceID := fmt.Sprintf(
		"valid-dispatch-%d",
		workflowID,
	)

	const leaseDuration = 30 * time.Second

	if err := RegisterSchedulerInstance(
		ctx,
		pool,
		instanceID,
		"valid-dispatch-host",
		leaseDuration,
	); err != nil {
		t.Fatalf("register scheduler: %v", err)
	}

	var (
		fencingToken    int64
		dispatchedJobID int64
	)

	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cleanupCancel()

		if fencingToken > 0 {
			_ = ReleaseSchedulerLeadership(
				cleanupCtx,
				pool,
				instanceID,
				fencingToken,
			)
		}

		_ = StopSchedulerInstance(
			cleanupCtx,
			pool,
			instanceID,
		)

		if dispatchedJobID > 0 {
			_, _ = pool.Exec(
				cleanupCtx,
				`DELETE FROM jobs WHERE id = $1`,
				dispatchedJobID,
			)
		}

		_, _ = pool.Exec(
			cleanupCtx,
			`DELETE FROM workflows WHERE id = $1`,
			workflowID,
		)

		_, _ = pool.Exec(
			cleanupCtx,
			`DELETE FROM scheduler_instances WHERE id = $1`,
			instanceID,
		)
	}()

	// Acquire leadership.
	leadership, err := AcquireSchedulerLeadership(
		ctx,
		pool,
		instanceID,
		leaseDuration,
	)
	if err != nil {
		t.Fatalf("acquire leadership: %v", err)
	}

	fencingToken = leadership.FencingToken

	// Create workflow.
	_, err = pool.Exec(
		ctx,
		`
		INSERT INTO workflows (id, name, status)
		VALUES ($1, $2, 'Pending')
		`,
		workflowID,
		"valid-leader-dispatch-workflow",
	)
	if err != nil {
		t.Fatalf("create workflow: %v", err)
	}

	// Create runnable task.
	_, err = pool.Exec(
		ctx,
		`
		INSERT INTO workflow_tasks (
			id,
			workflow_id,
			name,
			status
		)
		VALUES ($1, $2, $3, 'Pending')
		`,
		taskID,
		workflowID,
		"valid-leader-dispatch-task",
	)
	if err != nil {
		t.Fatalf("create task: %v", err)
	}

	// The dispatch function selects the lowest-ID runnable task
	// across all workflows. Confirm our fixture is next in line
	// so this test cannot accidentally dispatch unrelated work.
	var nextTaskID int64

	err = pool.QueryRow(
		ctx,
		`
		SELECT t.id
		FROM workflow_tasks t
		WHERE t.status = 'Pending'
		  AND NOT EXISTS (
			SELECT 1
			FROM task_dependencies d
			JOIN workflow_tasks dependency
			  ON dependency.id = d.depends_on_task_id
			WHERE d.task_id = t.id
			  AND dependency.status <> 'Completed'
		  )
		ORDER BY t.id
		LIMIT 1
		`,
	).Scan(&nextTaskID)

	if err != nil {
		t.Fatalf("find next runnable task: %v", err)
	}

	if nextTaskID != taskID {
		t.Skipf(
			"another runnable task %d has priority over fixture %d",
			nextTaskID,
			taskID,
		)
	}

	// Dispatch using the valid leader and its fencing token.
	dispatched, err := ClaimTaskAndCreateJobAsLeader(
		ctx,
		pool,
		instanceID,
		fencingToken,
	)
	if err != nil {
		t.Fatalf("leader dispatch: %v", err)
	}

	if dispatched == nil {
		t.Fatal("expected dispatched task, got nil")
	}

	dispatchedJobID = dispatched.JobID

	if dispatched.TaskID != taskID {
		t.Fatalf(
			"expected task ID %d, got %d",
			taskID,
			dispatched.TaskID,
		)
	}

	if dispatched.WorkflowID != workflowID {
		t.Fatalf(
			"expected workflow ID %d, got %d",
			workflowID,
			dispatched.WorkflowID,
		)
	}

	if dispatched.TaskName != "valid-leader-dispatch-task" {
		t.Fatalf(
			"unexpected task name: %q",
			dispatched.TaskName,
		)
	}

	// Verify task state and job link.
	var (
		taskStatus string
		jobID      *int64
	)

	err = pool.QueryRow(
		ctx,
		`
		SELECT status, job_id
		FROM workflow_tasks
		WHERE id = $1
		`,
		taskID,
	).Scan(&taskStatus, &jobID)

	if err != nil {
		t.Fatalf("read dispatched task: %v", err)
	}

	if taskStatus != "Ready" {
		t.Fatalf("expected Ready, got %q", taskStatus)
	}

	if jobID == nil || *jobID != dispatchedJobID {
		t.Fatalf("workflow task has incorrect job ID: %v", jobID)
	}

	// Verify the execution job.
	var jobName, jobStatus string

	err = pool.QueryRow(
		ctx,
		`
		SELECT name, status
		FROM jobs
		WHERE id = $1
		`,
		dispatchedJobID,
	).Scan(&jobName, &jobStatus)

	if err != nil {
		t.Fatalf("read execution job: %v", err)
	}

	if jobName != "valid-leader-dispatch-task" {
		t.Fatalf("unexpected job name: %q", jobName)
	}

	if jobStatus != "Pending" {
		t.Fatalf("expected job status Pending, got %q", jobStatus)
	}

	t.Logf(
		"leader %s successfully dispatched task %d as job %d with fencing token %d",
		instanceID,
		taskID,
		dispatchedJobID,
		fencingToken,
	)
}

func TestClaimTaskAndCreateJobAsLeaderRejectsPreviousLeaderAfterTakeover(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := godotenv.Load("../.env"); err != nil {
		t.Fatalf("load .env: %v", err)
	}

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("connect database: %v", err)
	}
	defer pool.Close()

	var leadershipAvailable bool
	err = pool.QueryRow(ctx, `
		SELECT instance_id IS NULL
		    OR lease_until <= clock_timestamp()
		FROM scheduler_leadership
		WHERE singleton_id = 1
	`).Scan(&leadershipAvailable)
	if err != nil {
		t.Fatalf("check leadership availability: %v", err)
	}
	if !leadershipAvailable {
		t.Skip("another scheduler currently owns active leadership")
	}

	uniqueID := time.Now().UnixNano()
	instanceA := fmt.Sprintf("takeover-a-%d", uniqueID)
	instanceB := fmt.Sprintf("takeover-b-%d", uniqueID)
	workflowID := uniqueID
	taskID := uniqueID + 1

	const leaseDuration = 30 * time.Second

	for _, instanceID := range []string{instanceA, instanceB} {
		if err := RegisterSchedulerInstance(
			ctx, pool, instanceID, "takeover-test-host", leaseDuration,
		); err != nil {
			t.Fatalf("register scheduler %s: %v", instanceID, err)
		}
	}

	var tokenA, tokenB int64
	var jobID int64

	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(), 5*time.Second,
		)
		defer cleanupCancel()

		if jobID > 0 {
			_, _ = pool.Exec(cleanupCtx, `DELETE FROM jobs WHERE id = $1`, jobID)
		}
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM workflows WHERE id = $1`, workflowID)

		_, _ = pool.Exec(cleanupCtx, `
			UPDATE scheduler_leadership
			SET instance_id = NULL, lease_until = NULL, updated_at = NOW()
			WHERE singleton_id = 1 AND instance_id = ANY($1)
		`, []string{instanceA, instanceB})

		for _, instanceID := range []string{instanceA, instanceB} {
			_ = StopSchedulerInstance(cleanupCtx, pool, instanceID)
			_, _ = pool.Exec(cleanupCtx,
				`DELETE FROM scheduler_instances WHERE id = $1`, instanceID)
		}
	}()

	leadershipA, err := AcquireSchedulerLeadership(
		ctx, pool, instanceA, leaseDuration,
	)
	if err != nil {
		t.Fatalf("scheduler A acquire leadership: %v", err)
	}
	tokenA = leadershipA.FencingToken

	// Expire A's leadership, while keeping its instance registered.
	_, err = pool.Exec(ctx, `
		UPDATE scheduler_leadership
		SET lease_until = clock_timestamp() - INTERVAL '1 second'
		WHERE singleton_id = 1 AND instance_id = $1
	`, instanceA)
	if err != nil {
		t.Fatalf("expire scheduler A leadership: %v", err)
	}

	leadershipB, err := AcquireSchedulerLeadership(
		ctx, pool, instanceB, leaseDuration,
	)
	if err != nil {
		t.Fatalf("scheduler B take over leadership: %v", err)
	}
	tokenB = leadershipB.FencingToken

	if tokenB <= tokenA {
		t.Fatalf("expected B token %d > A token %d", tokenB, tokenA)
	}

	// Create one runnable task for the new leader to dispatch.
	_, err = pool.Exec(ctx, `
		INSERT INTO workflows (id, name, status)
		VALUES ($1, $2, 'Pending')
	`, workflowID, "leadership-takeover-test")
	if err != nil {
		t.Fatalf("create workflow: %v", err)
	}

	_, err = pool.Exec(ctx, `
		INSERT INTO workflow_tasks (id, workflow_id, name, status)
		VALUES ($1, $2, $3, 'Pending')
	`, taskID, workflowID, "takeover-dispatch-task")
	if err != nil {
		t.Fatalf("create task: %v", err)
	}

	// The previous leader must be rejected.
	_, err = ClaimTaskAndCreateJobAsLeader(ctx, pool, instanceA, tokenA)
	if !errors.Is(err, ErrSchedulerLeaseLost) {
		t.Fatalf("expected old leader to be rejected, got: %v", err)
	}

	// The new leader must be able to dispatch.
	dispatched, err := ClaimTaskAndCreateJobAsLeader(
		ctx, pool, instanceB, tokenB,
	)
	if err != nil {
		t.Fatalf("new leader dispatch: %v", err)
	}
	if dispatched == nil {
		t.Fatal("expected new leader to dispatch a task")
	}
	if dispatched.TaskID != taskID {
		t.Fatalf("expected task %d, got %d", taskID, dispatched.TaskID)
	}

	jobID = dispatched.JobID

	t.Logf(
		"takeover verified: A token=%d rejected; B token=%d dispatched task=%d",
		tokenA, tokenB, taskID,
	)
}
