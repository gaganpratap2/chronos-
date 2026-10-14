
package database

import (
	"context"
	"fmt"

	"chronos/job"
	"github.com/jackc/pgx/v5/pgxpool"
)



// SaveJob stores a job in PostgreSQL.
func SaveJob(ctx context.Context, pool *pgxpool.Pool, j job.Job) error {
	_, err := pool.Exec(
		ctx,
		"INSERT INTO jobs (id, name, status) VALUES ($1, $2, $3)",
		// $1, $2, and $3 are PostgreSQL query parameters. They keep values separate from SQL code.
		// Scan(&j.ID, &j.Name, &j.Status) copies the database result into your Go struct fields.
		j.ID,
		j.Name,
		j.Status,
	)
	if err != nil {
		return fmt.Errorf("save job %d: %w", j.ID, err)
	}

	return nil
}

// GetJob retrieves a job from PostgreSQL by ID.
func GetJob(ctx context.Context, pool *pgxpool.Pool, id int) (job.Job, error) {
	var j job.Job

	err := pool.QueryRow(
		ctx,
		"SELECT id, name, status FROM jobs WHERE id = $1",
		id,
	).Scan(&j.ID, &j.Name, &j.Status)

	if err != nil {
		return job.Job{}, fmt.Errorf("get job %d: %w", id, err)
	}

	return j, nil
}
