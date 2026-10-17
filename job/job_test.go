
package job

import (

    "testing"
    "time"
)
func TestNewJob(t *testing.T) {
    j, err := NewJob(101, "payment")

    if err != nil {
        t.Fatalf("expected no error, got %v", err)
    }

    if j.ID != 101 {
        t.Errorf("expected ID 101, got %d", j.ID)
    }

    if j.Name != "payment" {
        t.Errorf("expected name payment, got %s", j.Name)
    }

    if j.Status != "Pending" {
        t.Errorf("expected status Pending, got %s", j.Status)
    }
}

func TestNewJobRejectsInvalidID(t *testing.T) {
    _, err := NewJob(0, "payment")

    if err == nil {
        t.Fatal("expected an error for ID 0")
    }
}

func TestNewJobRejectsEmptyName(t *testing.T) {
    _, err := NewJob(101, "")

    if err == nil {
        t.Fatal("expected an error for an empty name")
    }
}

func TestValidStatusTransitions(t *testing.T) {
    j, err := NewJob(101, "payment")
    if err != nil {
        t.Fatal(err)
    }

    if err := j.TransitionTo("Running"); err != nil {
        t.Fatalf("expected Pending -> Running to succeed: %v", err)
    }

    if j.Status != "Running" {
        t.Fatalf("expected Running, got %s", j.Status)
    }

    if err := j.TransitionTo("Completed"); err != nil {
        t.Fatalf("expected Running -> Completed to succeed: %v", err)
    }

    if j.Status != "Completed" {
        t.Errorf("expected Completed, got %s", j.Status)
    }
}

func TestInvalidStatusTransition(t *testing.T) {
    j, err := NewJob(101, "payment")
    if err != nil {
        t.Fatal(err)
    }

    err = j.TransitionTo("Completed")

    if err == nil {
        t.Fatal("expected an error for Pending -> Completed")
    }

    if j.Status != "Pending" {
        t.Errorf("expected status to remain Pending, got %s", j.Status)
    }
}

func TestRetryDelay(t *testing.T) {
	baseDelay := 500 * time.Millisecond
	maxDelay := 3 * time.Second

	tests := []struct {
		retryCount int
		expected   time.Duration
	}{
		{1, 500 * time.Millisecond},
		{2, 1 * time.Second},
		{3, 2 * time.Second},
		{4, 3 * time.Second}, // capped at maxDelay
		{5, 3 * time.Second}, // remains capped
	}

	for _, tt := range tests {
		got := RetryDelay(tt.retryCount, baseDelay, maxDelay)

		if got != tt.expected {
			t.Fatalf(
				"retry %d: expected %v, got %v",
				tt.retryCount,
				tt.expected,
				got,
			)
		}
	}
}