package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"chronos/database"
	"chronos/job"

	"github.com/joho/godotenv"
)

type fakeHandler struct {
	err   error
	calls int
}

func (f *fakeHandler) Execute(ctx context.Context, j job.Job) error {
	f.calls++
	return f.err
}

var _ Handler = (*fakeHandler)(nil)

func TestWorkerRejectsMissingDependencies(t *testing.T) {
	ctx := context.Background()

	w := &Worker{}

	ran, err := w.RunOnce(ctx)

	if err == nil {
		t.Fatal("expected error when database pool is nil")
	}

	if ran {
		t.Fatal("expected no job to run")
	}

	if err.Error() != "worker database pool is nil" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFakeHandler(t *testing.T) {
	handler := &fakeHandler{}

	j := job.Job{
		ID:     999,
		Name:   "test-job",
		Status: "Running",
	}

	err := handler.Execute(context.Background(), j)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if handler.calls != 1 {
		t.Fatalf("expected handler to be called once, got %d", handler.calls)
	}
}

var errTestExecution = errors.New("test execution failed")

func TestWorkerExecutesJobSuccessfully(t *testing.T) {
	if err := godotenv.Load("../.env"); err != nil {
		t.Log(".env not found; using system environment variables")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	pool, err := database.Connect(ctx)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()

	const testID = 2

	// Remove the test job before and after the test.
	_, err = pool.Exec(ctx, "DELETE FROM jobs WHERE id = $1", testID)
	if err != nil {
		t.Fatalf("failed to prepare test: %v", err)
	}

	defer func() {
		_, _ = pool.Exec(
			context.Background(),
			"DELETE FROM jobs WHERE id = $1",
			testID,
		)
	}()

	j, err := job.NewJob(testID, "worker-success-test")
	if err != nil {
		t.Fatal(err)
	}

	if err := database.SaveJob(ctx, pool, j); err != nil {
		t.Fatalf("SaveJob failed: %v", err)
	}

	handler := &fakeHandler{}

	w := &Worker{
		Pool:    pool,
		Handler: handler,
	}

	ran, err := w.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce failed: %v", err)
	}

	if !ran {
		t.Fatal("expected worker to run a job")
	}

	if handler.calls != 1 {
		t.Fatalf("expected handler to be called once, got %d", handler.calls)
	}

	saved, err := database.GetJob(ctx, pool, testID)
	if err != nil {
		t.Fatalf("GetJob failed: %v", err)
	}

	if saved.Status != "Completed" {
		t.Fatalf("expected database status Completed, got %s", saved.Status)
	}
}

func TestWorkerMarksJobFailedWhenHandlerFails(t *testing.T) {
	if err := godotenv.Load("../.env"); err != nil {
		t.Log(".env not found; using system environment variables")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)

	defer cancel()

	pool, err := database.Connect(ctx)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()

	const testID = 3
	_, err = pool.Exec(ctx, "DELETE FROM jobs WHERE id = $1", testID)
	if err != nil {
		t.Fatalf("failed to prepare test: %v", err)
	}

	defer func() {
		_, _ = pool.Exec(
			context.Background(),
			"DELETE FROM jobs WHERE id = $1",
			testID,
		)
	}()

	j, err := job.NewJob(testID, "worker-failure-test")
	if err != nil {
		t.Fatal(err)
	}

	if err := database.SaveJob(ctx, pool, j); err != nil {
		t.Fatalf("SaveJob failed: %v", err)
	}

	handler := &fakeHandler{
		err: errTestExecution,
	}

	w := &Worker{
		Pool:    pool,
		Handler: handler,
	}

	ran, err := w.RunOnce(ctx)

	if err == nil {
		t.Fatal("expected RunOnce to return an execution error")
	}

	if !ran {
		t.Fatal("expected worker to run a job")
	}

	if handler.calls != 1 {
		t.Fatalf("expected handler to be called once, got %d", handler.calls)
	}

	saved, err := database.GetJob(ctx, pool, testID)
	if err != nil {
		t.Fatalf("GetJob failed: %v", err)
	}

	if saved.Status != "Failed" {
		t.Fatalf("expected database status Failed, got %s", saved.Status)
	}
}
