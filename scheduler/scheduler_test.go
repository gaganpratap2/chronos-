package scheduler

import (
	"context"
	"testing"
	"time"
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
