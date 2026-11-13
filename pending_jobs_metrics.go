package main

import (
	"context"
	"time"

	"chronos/database"
	"chronos/observability"

	"github.com/jackc/pgx/v5/pgxpool"
	"log/slog"
)

// runPendingJobsMetrics periodically refreshes the number of pending jobs.
func runPendingJobsMetrics(
	ctx context.Context,
	pool *pgxpool.Pool,
	metrics *observability.Metrics,
	interval time.Duration,
) {
	if interval <= 0 {
		interval = 15 * time.Second
	}

	refresh := func() {
		count, err := database.CountPendingJobs(ctx, pool)
		if err != nil {
			if ctx.Err() == nil {
				slog.Error(
					"failed to refresh pending jobs metric",
					"error", err,
				)
			}
			return
		}

		metrics.PendingJobs.Set(float64(count))
	}

	// Populate the gauge immediately at startup.
	refresh()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			refresh()
		}
	}
}
