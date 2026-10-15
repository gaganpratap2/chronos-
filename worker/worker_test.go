package worker

import (
	"context"
	"errors"
	"testing"

	"chronos/job"
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
