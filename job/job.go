package job

import (
	"errors"
	"fmt"
	"math/rand"
	"time"
)

var ErrInvalidTransition = errors.New("invalid job status transition")
var ErrRetriesExhausted = errors.New("job has exhausted its retries")

type Job struct {
	ID          int
	Name        string
	Status      string
	RetryCount  int
	MaxRetries  int
	NextRetryAt *time.Time
}

type RetryPolicy struct {
	MaxRetries    int
	BaseDelay     time.Duration
	MaxDelay      time.Duration
	JitterPercent float64
}

func NewJob(id int, name string) (Job, error) {
	if id <= 0 {
		return Job{}, errors.New("job ID must be positive")
	}

	if name == "" {
		return Job{}, errors.New("job name cannot be empty")
	}

	return Job{
		ID:         id,
		Name:       name,
		Status:     "Pending",
		RetryCount: 0,
		MaxRetries: 3,
	}, nil
}

func CompletedJob(job Job) Job {
	job.Status = "Completed"
	return job
}

func (j *Job) TransitionTo(nextStatus string) error {
	if j.Status == "Pending" && nextStatus == "Running" {
		j.Status = nextStatus
		return nil
	}

	if j.Status == "Running" && nextStatus == "Completed" {
		j.Status = nextStatus
		return nil
	}

	if j.Status == "Running" && nextStatus == "Failed" {
		j.Status = nextStatus
		return nil
	}

	return fmt.Errorf("%w: %s -> %s",
		ErrInvalidTransition, j.Status, nextStatus)
}

func (j *Job) Retry() error {
	if j.Status != "Failed" {
		return fmt.Errorf(
			"cannot retry job with status %s", j.Status)
	}

	if j.RetryCount >= j.MaxRetries {
		return ErrRetriesExhausted
	}

	j.RetryCount++
	j.Status = "Pending"

	return nil
}

func RetryDelay(retryCount int, policy RetryPolicy) time.Duration {
	if retryCount <= 0 {
		return 0
	}

	// delay := baseDelay * time.Duration(1<<(retryCount-1))
	delay := policy.BaseDelay * time.Duration(1<<(retryCount-1))

	if delay > policy.MaxDelay {
		return policy.MaxDelay
	}

	return delay
}

func ApplyJitter(delay time.Duration, policy RetryPolicy) time.Duration {
	if delay <= 0 || policy.JitterPercent <= 0 {
		return delay
	}

	jitter := float64(delay) * policy.JitterPercent
	offset := (rand.Float64()*2 - 1) * jitter // rand.Float64() generates a random number between 0 and 1.
	return delay + time.Duration(offset)
}
