package database

import (
	"context"
	"database/sql"
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
		j.ID, j.Name, j.Status, j.RetryCount, j.MaxRetries, j.NextRetryAt,
	)
	if err != nil {
		return fmt.Errorf("save job %d: %w", j.ID, err)
	}

	return nil
}

// GetJob retrieves a job from PostgreSQL by ID.
func GetJob(ctx context.Context, pool *pgxpool.Pool, id int) (job.Job, error) {

	var j job.Job
	var workerID sql.NullString

	err := pool.QueryRow(ctx,
		`SELECT id, name, status, retry_count, max_retries , next_retry_at , worker_id ,lease_until , fencing_token
        FROM jobs WHERE id = $1`,
		id,
	).Scan(&j.ID,
		&j.Name,
		&j.Status,
		&j.RetryCount,
		&j.MaxRetries,
		&j.NextRetryAt,
		&workerID,
		&j.LeaseUntil,
		&j.FencingToken,
	)

	if err != nil {
		return job.Job{}, fmt.Errorf("get job %d: %w", id, err)
	}

	if workerID.Valid {
		j.WorkerID = workerID.String
	}

	return j, nil
}

func TransitionJobStatus(
	ctx context.Context,
	pool *pgxpool.Pool,
	id int,
	nextStatus string,
	fencingToken int64,
) (job.Job, error) {

	tx, err := pool.Begin(ctx)
	if err != nil {
		return job.Job{}, fmt.Errorf("begin status transition: %w", err)
	}
	defer tx.Rollback(ctx)

	var current job.Job
	// tx: ==> A transaction is a group of database operations treated as one unit. You can commit the changes or roll them back.
	err = tx.QueryRow(ctx,
		`SELECT id, name, status, retry_count, max_retries, next_retry_at , fencing_token
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
		&current.FencingToken,
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

	result, err := tx.Exec(ctx,
		`UPDATE jobs
     	SET status = $1, next_retry_at = $2
     	WHERE id = $3
       AND fencing_token = $4`,
		current.Status,
		current.NextRetryAt,
		current.ID,
		fencingToken,
	)

	if err != nil {
		return job.Job{}, fmt.Errorf("save job status: %w", err)
	}

	if result.RowsAffected() != 1 {
		return job.Job{}, fmt.Errorf("stale fencing token for job %d", id)
	}

	if err := tx.Commit(ctx); err != nil {
		return job.Job{}, fmt.Errorf("commit status transition: %w", err)
	}

	return current, nil
}

func RetryJob(ctx context.Context, pool *pgxpool.Pool, id int, policy job.RetryPolicy, fencingToken int64) (job.Job, error) {

	tx, err := pool.Begin(ctx)
	if err != nil {
		return job.Job{}, fmt.Errorf("begin retry: %w", err)
	}
	defer tx.Rollback(ctx)

	var current job.Job

	err = tx.QueryRow(ctx,
		`SELECT id, name, status, retry_count, max_retries, next_retry_at , fencing_token
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
		&current.FencingToken,
	)
	if err != nil {
		return job.Job{}, fmt.Errorf("load job for retry: %w", err)
	}

	if err := current.Retry(); err != nil {
		return job.Job{}, fmt.Errorf("retry job %d: %w", id, err)
	}

	// nextRetryAt := time.Now().Add(1 * time.Second)

	delay := job.RetryDelay(current.RetryCount, policy)
	delay = job.ApplyJitter(delay, policy)
	nextRetryAt := time.Now().Add(delay)

	current.NextRetryAt = &nextRetryAt

	result, err := tx.Exec(ctx,
		`UPDATE jobs
 		SET status = $1, retry_count = $2, next_retry_at = $3
 		WHERE id = $4
   			AND fencing_token = $5`,
		current.Status,
		current.RetryCount,
		current.NextRetryAt,
		current.ID,
		fencingToken,
	)
	if err != nil {
		return job.Job{}, fmt.Errorf("save job retry: %w", err)
	}

	if result.RowsAffected() != 1 {
		return job.Job{}, fmt.Errorf("stale fencing token for job %d", id)
	}

	if err := tx.Commit(ctx); err != nil {
		return job.Job{}, fmt.Errorf("commit job retry: %w", err)
	}

	return current, nil
}

func ClaimNextJob(ctx context.Context, pool *pgxpool.Pool, workerID string) (job.Job, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return job.Job{}, fmt.Errorf("begin claim transaction: %w", err)
	}

	defer tx.Rollback(ctx)

	var current job.Job

	err = tx.QueryRow(ctx, `
                    SELECT id , name , status , retry_count , max_retries , next_retry_at, fencing_token
                    FROM jobs 
                    WHERE 
						(
       						status = 'Pending'
        					AND (next_retry_at IS NULL OR next_retry_at <= NOW())
    					)
    					OR
    					(
        					status = 'Running'
        					AND lease_until <= NOW()
    					)
                    ORDER BY id 
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED `,
	).Scan(&current.ID,
		&current.Name,
		&current.Status,
		&current.RetryCount,
		&current.MaxRetries,
		&current.NextRetryAt,
		&current.FencingToken,
	)

	if err != nil {
		return job.Job{}, fmt.Errorf("find pending job: %w", err)
	}

	if current.Status == "Pending" {
		if err := current.TransitionTo("Running"); err != nil {
			return job.Job{}, fmt.Errorf("claim job: %w", err)
		}
	}

	err = tx.QueryRow(ctx, `
    UPDATE jobs
    SET status = 'Running',
        worker_id = $2,
		lease_until = NOW() + INTERVAL '30 seconds',
        fencing_token = fencing_token + 1
    WHERE id = $1
    RETURNING fencing_token
`, current.ID, workerID).Scan(&current.FencingToken)

	if err != nil {
		return job.Job{}, fmt.Errorf("update claimed job: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return job.Job{}, fmt.Errorf("commit claim transaction: %w", err)
	}

	return current, nil
}

func RenewJobLease(ctx context.Context, pool *pgxpool.Pool, id int, fencingToken int64) error {

	result, err := pool.Exec(ctx,
		`UPDATE jobs
		 SET lease_until = NOW() + INTERVAL '30 seconds'
		 WHERE id = $1
		   AND status = 'Running'
		   AND fencing_token = $2`,
		id,
		fencingToken,
	)
	if err != nil {
		return fmt.Errorf("renew job lease: %w", err)
	}

	if result.RowsAffected() != 1 {
		return fmt.Errorf("stale fencing token or job is not running")
	}

	return nil
}

func CreateExecutionJob(
	ctx context.Context,
	pool *pgxpool.Pool,
	id int,
	name string,
) (job.Job, error) {
	executionJob, err := job.NewJob(id, name)
	if err != nil {
		return job.Job{}, fmt.Errorf(
			"create execution job: %w",
			err,
		)
	}

	if err := SaveJob(ctx, pool, executionJob); err != nil {
		return job.Job{}, fmt.Errorf(
			"save execution job: %w",
			err,
		)
	}

	return executionJob, nil
}
