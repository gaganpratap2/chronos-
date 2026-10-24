package scheduler

import (
	"context"
	"time"

	"chronos/database"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Scheduler struct {
	Pool         *pgxpool.Pool
	PollInterval time.Duration
}

func New(pool *pgxpool.Pool) *Scheduler {
	return &Scheduler{
		Pool:         pool,
		PollInterval: 500 * time.Millisecond,
	}
}

func (s *Scheduler) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-ticker.C:
			if err := s.RunOnce(ctx); err != nil {
				return err
			}
		}
	}
}

func (s *Scheduler) RunOnce(ctx context.Context) error {
	_, err := database.ClaimTaskAndCreateJob(
		ctx, s.Pool,
	)

	return err
}
