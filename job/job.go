
package job

import (
    "errors"
    "fmt"
)

var ErrInvalidTransition = errors.New("invalid job status transition")


type Job struct {
    ID         int
    Name       string
    Status     string
    RetryCount int
    MaxRetries int
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
            "cannot retry job with status %s",
            j.Status,
        )
    }

    if j.RetryCount >= j.MaxRetries {
        return fmt.Errorf("job has exhausted its retries")
    }

    j.RetryCount++
    j.Status = "Pending"

    return nil
}
