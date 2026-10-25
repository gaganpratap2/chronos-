package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"chronos/database"

	"github.com/joho/godotenv"
)

func TestSchedulerStopsWhenContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	s := &Scheduler{
		PollInterval: 10 * time.Millisecond,
	}

	done := make(chan error, 1)

	go func() {
		done <- s.Run(ctx) //The test can now wait for the scheduler to finish without blocking the goroutine that started it.
	}()

	cancel()

	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf(
				"expected context.Canceled, got %v",
				err,
			)
		}

	case <-time.After(1 * time.Second):
		t.Fatal("scheduler did not stop after cancellation")
	}
}

func TestSchedulerRunOnceDispatchesTask(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	if err := godotenv.Load("../.env"); err != nil {
		t.Fatalf("load .env: %v", err)
	}

	pool, err := database.Connect(ctx)
	if err != nil {
		t.Fatalf("connect database: %v", err)
	}
	defer pool.Close()

	const (
		workflowID = int64(999301)
		taskID     = int64(999302)
	)

	_, err = pool.Exec(
		ctx,
		`DELETE FROM workflows WHERE id = $1`,
		workflowID,
	)
	if err != nil {
		t.Fatalf("cleanup before test: %v", err)
	}

	_, err = pool.Exec(
		ctx,
		`
		INSERT INTO workflows (
			id,
			name,
			status
		)
		VALUES ($1, $2, 'Pending')
		`,
		workflowID,
		"scheduler-dispatch-test",
	)
	if err != nil {
		t.Fatalf("create workflow: %v", err)
	}

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
		"scheduler-test-task",
	)
	if err != nil {
		t.Fatalf("create task: %v", err)
	}

	s := New(pool)

	err = s.RunOnce(ctx)
	if err != nil {
		t.Fatalf("scheduler RunOnce: %v", err)
	}

	// Verify task was dispatched.
	var (
		status string
		jobID  *int64
	)

	err = pool.QueryRow(
		ctx,
		`
		SELECT status, job_id
		FROM workflow_tasks
		WHERE id = $1
		`,
		taskID,
	).Scan(
		&status,
		&jobID,
	)

	if err != nil {
		t.Fatalf("read task: %v", err)
	}

	if status != "Ready" {
		t.Fatalf(
			"expected task status Ready, got %q",
			status,
		)
	}

	if jobID == nil {
		t.Fatal("expected task to have job_id")
	}

	if *jobID <= 0 {
		t.Fatalf(
			"expected valid job ID, got %d",
			*jobID,
		)
	}

	var jobStatus string

	err = pool.QueryRow(
		ctx,
		`
		SELECT status
		FROM jobs
		WHERE id = $1
		`,
		*jobID,
	).Scan(&jobStatus)

	if err != nil {
		t.Fatalf("read execution job: %v", err)
	}

	if jobStatus != "Pending" {
		t.Fatalf(
			"expected job status Pending, got %q",
			jobStatus,
		)
	}

	// Cleanup job.
	_, err = pool.Exec(
		ctx,
		`DELETE FROM jobs WHERE id = $1`,
		*jobID,
	)
	if err != nil {
		t.Fatalf("cleanup job: %v", err)
	}

	// Cleanup workflow and task.
	_, err = pool.Exec(
		ctx,
		`DELETE FROM workflows WHERE id = $1`,
		workflowID,
	)
	if err != nil {
		t.Fatalf("cleanup workflow: %v", err)
	}
}

func TestSchedulerStopsOnContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	s := &Scheduler{
		PollInterval: 10 * time.Millisecond,
	}

	cancel()

	err := s.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestSchedulerStopsWhileRunning(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := &Scheduler{
		PollInterval: time.Hour,
	}

	done := make(chan error, 1)

	go func() {
		done <- s.Run(ctx)
	}()

	time.Sleep(20 * time.Millisecond)

	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}

	case <-time.After(time.Second):
		t.Fatal("scheduler did not stop after context cancellation")
	}
}

func TestSchedulerRunsOnTick(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	called := make(chan struct{}, 1)

	s := &Scheduler{
		PollInterval: 10 * time.Millisecond,
		RunOnceFunc: func(ctx context.Context) error {
			called <- struct{}{}
			return nil
		},
	}

	done := make(chan error, 1)

	go func() {
		done <- s.Run(ctx)
	}()

	select {
	case <-called:
		cancel()

	case <-time.After(time.Second):
		t.Fatal("scheduler did not run on tick")
	}

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}

	case <-time.After(time.Second):
		t.Fatal("scheduler did not stop after cancellation")
	}
}

func TestSchedulerPropagatesRunOnceError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	expectedErr := errors.New("scheduler failure")

	s := &Scheduler{
		PollInterval: 10 * time.Millisecond,
		RunOnceFunc: func(ctx context.Context) error {
			return expectedErr
		},
	}

	err := s.Run(ctx)

	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected %v, got %v", expectedErr, err)
	}
}
