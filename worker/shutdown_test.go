package worker

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNewExecutionContextDrainsUntilTimeout(t *testing.T) {
	parentCtx, cancelParent := context.WithCancel(context.Background())
	executionCtx, finish := newExecutionContext(
		parentCtx,
		100*time.Millisecond,
	)
	defer finish()

	cancelParent()

	// The active job should remain alive during the drain period.
	select {
	case <-executionCtx.Done():
		t.Fatal("execution context cancelled before drain timeout")
	case <-time.After(25 * time.Millisecond):
	}

	// It must be cancelled when the drain timeout expires.
	select {
	case <-executionCtx.Done():
		if !errors.Is(executionCtx.Err(), context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", executionCtx.Err())
		}
	case <-time.After(time.Second):
		t.Fatal("execution context was not cancelled after drain timeout")
	}
}

func TestNewExecutionContextFinishesBeforeTimeout(t *testing.T) {
	parentCtx, cancelParent := context.WithCancel(context.Background())
	executionCtx, finish := newExecutionContext(
		parentCtx,
		time.Second,
	)
	defer finish()

	cancelParent()

	// Simulate the active job completing during the drain period.
	finish()

	select {
	case <-executionCtx.Done():
		if !errors.Is(executionCtx.Err(), context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", executionCtx.Err())
		}
	case <-time.After(time.Second):
		t.Fatal("execution context was not cancelled after finishing")
	}
}

func TestNewExecutionContextWithoutDrainTimeout(t *testing.T) {
	parentCtx, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()

	executionCtx, finish := newExecutionContext(
		parentCtx,
		0,
	)
	defer finish()

	cancelParent()

	select {
	case <-executionCtx.Done():
		if !errors.Is(executionCtx.Err(), context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", executionCtx.Err())
		}
	case <-time.After(time.Second):
		t.Fatal("execution context should follow parent cancellation")
	}
}
