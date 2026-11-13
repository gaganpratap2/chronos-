package database

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// CountPendingJobs returns the number of jobs waiting to be processed.
func CountPendingJobs(
	ctx context.Context,
	pool *pgxpool.Pool,
) (int64, error) {
	var count int64

	err := pool.QueryRow(
		ctx,
		`SELECT COUNT(*) FROM jobs WHERE status = 'Pending'`,
	).Scan(&count)

	if err != nil {
		return 0, err
	}

	return count, nil
}
