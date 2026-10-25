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
	RunOnceFunc  func(context.Context) error
}

func New(pool *pgxpool.Pool) *Scheduler {
	return &Scheduler{
		Pool:         pool,
		PollInterval: 500 * time.Millisecond,
	}
}

func (s *Scheduler) Run(ctx context.Context) error {
	interval := s.PollInterval
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-ticker.C:
			runOnce := s.RunOnce
			if s.RunOnceFunc != nil {
				runOnce = s.RunOnceFunc
			}

			if err := runOnce(ctx); err != nil {
				return err
			}
		}
	}
}

func (s *Scheduler) RunOnce(ctx context.Context) error {
	_, err := database.ClaimTaskAndCreateJob(
		ctx,
		s.Pool,
	)

	return err
}
