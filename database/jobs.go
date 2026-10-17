package database

import (
	"context"
	"fmt"
	"time"

	"chronos/job"
	"github.com/jackc/pgx/v5/pgxpool"
)

// SaveJob stores a job in PostgreSQL.
func SaveJob(ctx context.Context, pool *pgxpool.Pool, j job.Job) error {
	_, err := pool.Exec(ctx,
		`INSERT INTO jobs (id, name, status, retry_count, max_retries , next_retry_at)
         VALUES ($1, $2, $3, $4, $5 , $6)`,
		j.ID, j.Name, j.Status, j.RetryCount, j.MaxRetries , j.NextRetryAt,
	)
	if err != nil {
		return fmt.Errorf("save job %d: %w", j.ID, err)
	}

	return nil
}

// GetJob retrieves a job from PostgreSQL by ID.
func GetJob(ctx context.Context, pool *pgxpool.Pool, id int) (job.Job, error) {
	var j job.Job

	err := pool.QueryRow(ctx,
		`SELECT id, name, status, retry_count, max_retries , next_retry_at
        FROM jobs WHERE id = $1`,
		id,
	).Scan(&j.ID, &j.Name, &j.Status, &j.RetryCount, &j.MaxRetries , &j.NextRetryAt)

	if err != nil {
		return job.Job{}, fmt.Errorf("get job %d: %w", id, err)
	}

	return j, nil
}

func TransitionJobStatus(
	ctx context.Context,
	pool *pgxpool.Pool,
	id int,
	nextStatus string,
) (job.Job, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return job.Job{}, fmt.Errorf("begin status transition: %w", err)
	}
	defer tx.Rollback(ctx)

	var current job.Job
	// tx: ==> A transaction is a group of database operations treated as one unit. You can commit the changes or roll them back.
	err = tx.QueryRow(ctx,
		`SELECT id, name, status, retry_count, max_retries, next_retry_at
     FROM jobs
     WHERE id = $1
     FOR UPDATE`,
		id,
	).Scan(
		&current.ID,
		&current.Name,
		&current.Status,
		&current.RetryCount,
		&current.MaxRetries,
		&current.NextRetryAt,
	)

	if err != nil {
		return job.Job{}, fmt.Errorf("load job for transition: %w", err)
	}

	if err := current.TransitionTo(nextStatus); err != nil {
		return job.Job{}, fmt.Errorf("transition job %d: %w", id, err)
	}

	if nextStatus == "Completed" {
    	current.NextRetryAt = nil
	}

	_, err = tx.Exec(ctx,
    	`UPDATE jobs
     	SET status = $1, next_retry_at = $2
     	WHERE id = $3`,
    	current.Status,
    	current.NextRetryAt,
    	current.ID,
	)

	if err != nil {
		return job.Job{}, fmt.Errorf("save job status: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return job.Job{}, fmt.Errorf("commit status transition: %w", err)
	}

	return current, nil
}

func RetryJob( ctx context.Context, pool *pgxpool.Pool, id int ) (job.Job, error) {

	tx, err := pool.Begin(ctx)
	if err != nil {
		return job.Job{}, fmt.Errorf("begin retry: %w", err)
	}
	defer tx.Rollback(ctx)

	var current job.Job

	err = tx.QueryRow(ctx,
		`SELECT id, name, status, retry_count, max_retries, next_retry_at
         FROM jobs
         WHERE id = $1
         FOR UPDATE`,
		id,
	).Scan(
		&current.ID,
		&current.Name,
		&current.Status,
		&current.RetryCount,
		&current.MaxRetries,
		&current.NextRetryAt,
	)	
	if err != nil {
		return job.Job{}, fmt.Errorf("load job for retry: %w", err)
	}

	if err := current.Retry(); err != nil {
		return job.Job{}, fmt.Errorf("retry job %d: %w", id, err)
	}

	nextRetryAt := time.Now().Add(1 * time.Second)
		current.NextRetryAt = &nextRetryAt

	_, err = tx.Exec(ctx,
		`UPDATE jobs
         SET status = $1, retry_count = $2 , next_retry_at = $3
         WHERE id = $4`,
		current.Status, current.RetryCount, current.NextRetryAt, current.ID,
	)
	if err != nil {
		return job.Job{}, fmt.Errorf("save job retry: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return job.Job{}, fmt.Errorf("commit job retry: %w", err)
	}

	return current, nil
}





func ClaimNextJob(ctx context.Context, pool *pgxpool.Pool) (job.Job, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return job.Job{}, fmt.Errorf("begin claim transaction: %w", err)
	}

	defer tx.Rollback(ctx)

	var current job.Job

	err = tx.QueryRow(ctx, `
                    SELECT id , name , status , retry_count , max_retries
                    FROM jobs 
                    WHERE status = 'Pending'
  					AND (next_retry_at IS NULL OR next_retry_at <= NOW())
                    ORDER BY id 
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED `,
	).Scan(&current.ID,
		&current.Name,
		&current.Status,
		&current.RetryCount,
		&current.MaxRetries)

	if err != nil {
		return job.Job{}, fmt.Errorf("find pending job: %w", err)
	}

	if err := current.TransitionTo("Running"); err != nil {
		return job.Job{}, fmt.Errorf("claim job: %w", err)
	}

	_, err = tx.Exec(ctx, `
                    UPDATE jobs
                    SET status = $1
                    WHERE id = $2        
        `, current.Status, current.ID,
	)

	if err != nil {
		return job.Job{}, fmt.Errorf("update claimed job: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return job.Job{}, fmt.Errorf("commit claim transaction: %w", err)
	}

	return current, nil
}
