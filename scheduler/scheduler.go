package scheduler

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"chronos/database"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Scheduler struct {
	Pool              *pgxpool.Pool
	PollInterval      time.Duration
	RunOnceFunc       func(context.Context) error
	InstanceID        string
	Hostname          string
	LeaseDuration     time.Duration
	HeartbeatInterval time.Duration
}

func New(pool *pgxpool.Pool) *Scheduler {
	hostname, _ := os.Hostname()

	return &Scheduler{
		Pool:              pool,
		PollInterval:      500 * time.Millisecond,
		InstanceID:        fmt.Sprintf("%s-%d", hostname, time.Now().UnixNano()),
		Hostname:          hostname,
		LeaseDuration:     30 * time.Second,
		HeartbeatInterval: 10 * time.Second,
	}
}

func (s *Scheduler) Run(ctx context.Context) error {
	if s == nil {
		return errors.New("scheduler is nil")
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	useInstanceRegistry := s.Pool != nil

	pollInterval := s.PollInterval
	if pollInterval <= 0 {
		pollInterval = 500 * time.Millisecond
	}

	leaseDuration := s.LeaseDuration
	if leaseDuration <= 0 {
		leaseDuration = 30 * time.Second
	}

	heartbeatInterval := s.HeartbeatInterval
	if heartbeatInterval <= 0 {
		heartbeatInterval = leaseDuration / 3
	}

	if heartbeatInterval <= 0 || heartbeatInterval >= leaseDuration {
		return errors.New(
			"scheduler heartbeat interval must be positive and shorter than lease duration",
		)
	}

	var fencingToken int64

	// A scheduler without leadership is allowed to remain alive
	// as a standby instance.
	tryAcquireLeadership := func() error {
		leadership, err := database.AcquireSchedulerLeadership(
			ctx,
			s.Pool,
			s.InstanceID,
			leaseDuration,
		)

		if errors.Is(
			err,
			database.ErrSchedulerLeadershipNotAcquired,
		) {
			return nil
		}

		if err != nil {
			return fmt.Errorf(
				"acquire scheduler leadership: %w",
				err,
			)
		}

		fencingToken = leadership.FencingToken

		fmt.Printf(
			"scheduler %s became leader with fencing token %d\n",
			s.InstanceID,
			fencingToken,
		)

		return nil
	}

	if useInstanceRegistry {
		if s.InstanceID == "" || s.Hostname == "" {
			return errors.New(
				"scheduler instance ID and hostname are required",
			)
		}

		if err := database.RegisterSchedulerInstance(
			ctx,
			s.Pool,
			s.InstanceID,
			s.Hostname,
			leaseDuration,
		); err != nil {
			return fmt.Errorf(
				"register scheduler instance: %w",
				err,
			)
		}

		// Release leadership before marking this instance stopped.
		defer func() {
			stopCtx, cancel := context.WithTimeout(
				context.Background(),
				5*time.Second,
			)
			defer cancel()

			if fencingToken > 0 {
				if err := database.ReleaseSchedulerLeadership(
					stopCtx,
					s.Pool,
					s.InstanceID,
					fencingToken,
				); err != nil {
					fmt.Printf(
						"release scheduler leadership: %v\n",
						err,
					)
				}

				fencingToken = 0
			}

			if err := database.StopSchedulerInstance(
				stopCtx,
				s.Pool,
				s.InstanceID,
			); err != nil {
				fmt.Printf(
					"stop scheduler instance: %v\n",
					err,
				)
			}
		}()

		// Try to become leader immediately.
		if err := tryAcquireLeadership(); err != nil {
			return err
		}
	}

	pollTicker := time.NewTicker(pollInterval)
	defer pollTicker.Stop()

	heartbeatTicker := time.NewTicker(heartbeatInterval)
	defer heartbeatTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-heartbeatTicker.C:
			if !useInstanceRegistry {
				continue
			}

			// First renew this scheduler's instance lease.
			if err := database.RenewSchedulerInstanceLease(
				ctx,
				s.Pool,
				s.InstanceID,
				leaseDuration,
			); err != nil {
				return fmt.Errorf(
					"renew scheduler instance lease: %w",
					err,
				)
			}

			if fencingToken > 0 {
				// We currently believe we are the leader.
				err := database.RenewSchedulerLeadership(
					ctx,
					s.Pool,
					s.InstanceID,
					fencingToken,
					leaseDuration,
				)

				if errors.Is(err, database.ErrSchedulerLeaseLost) {
					fmt.Printf(
						"scheduler %s lost leadership\n",
						s.InstanceID,
					)

					fencingToken = 0
					continue
				}

				if err != nil {
					return fmt.Errorf(
						"renew scheduler leadership: %w",
						err,
					)
				}
			}

			if fencingToken == 0 {
				// Standby scheduler: try to acquire leadership.
				if err := tryAcquireLeadership(); err != nil {
					return err
				}
			}

		case <-pollTicker.C:
			if useInstanceRegistry && fencingToken == 0 {
				// Standby schedulers must never dispatch.
				continue
			}

			if s.RunOnceFunc != nil {
				// Injected hook for runtime tests.
				if err := s.RunOnceFunc(ctx); err != nil {
					return fmt.Errorf(
						"scheduler dispatch: %w",
						err,
					)
				}

				continue
			}

			if s.Pool == nil {
				continue
			}

			// Real dispatch must validate leadership and the
			// fencing token inside its database transaction.
			_, err := database.ClaimTaskAndCreateJobAsLeader(
				ctx,
				s.Pool,
				s.InstanceID,
				fencingToken,
			)

			if errors.Is(err, database.ErrSchedulerLeaseLost) {
				fmt.Printf(
					"scheduler %s stopped dispatching after losing leadership\n",
					s.InstanceID,
				)

				fencingToken = 0
				continue
			}

			if err != nil {
				return fmt.Errorf(
					"scheduler dispatch: %w",
					err,
				)
			}
		}
	}
}

func (s *Scheduler) RunOnce(ctx context.Context) error {
	if s == nil {
		return errors.New("scheduler is nil")
	}
	if ctx == nil {
		return errors.New("scheduler context is nil")
	}
	if s.Pool == nil {
		return errors.New("scheduler database pool is nil")
	}
	if s.InstanceID == "" {
		return errors.New("scheduler instance ID is empty")
	}

	leaseDuration := s.LeaseDuration
	if leaseDuration <= 0 {
		leaseDuration = 30 * time.Second
	}

	hostname := s.Hostname
	if hostname == "" {
		hostname, _ = os.Hostname()
	}
	if hostname == "" {
		hostname = "unknown"
	}

	if err := database.RegisterSchedulerInstance(
		ctx,
		s.Pool,
		s.InstanceID,
		hostname,
		leaseDuration,
	); err != nil {
		return fmt.Errorf("register scheduler instance: %w", err)
	}

	defer func() {
		cleanupCtx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()

		_ = database.StopSchedulerInstance(
			cleanupCtx,
			s.Pool,
			s.InstanceID,
		)
	}()

	leadership, err := database.AcquireSchedulerLeadership(
		ctx,
		s.Pool,
		s.InstanceID,
		leaseDuration,
	)
	if err != nil {
		return fmt.Errorf("acquire scheduler leadership: %w", err)
	}

	defer func() {
		cleanupCtx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()

		_ = database.ReleaseSchedulerLeadership(
			cleanupCtx,
			s.Pool,
			s.InstanceID,
			leadership.FencingToken,
		)
	}()

	_, err = database.ClaimTaskAndCreateJobAsLeader(
		ctx,
		s.Pool,
		s.InstanceID,
		leadership.FencingToken,
	)
	if err != nil {
		return fmt.Errorf("scheduler dispatch: %w", err)
	}

	return nil
}

// func (s *Scheduler) Run(ctx context.Context) error {
// 	// Respect cancellation before doing any database work.
// 	if err := ctx.Err(); err != nil {
// 		return err
// 	}

// 	if s == nil {
// 		return errors.New("scheduler is nil")
// 	}

// 	// RunOnceFunc is an injected execution hook used by tests.
// 	// Real scheduler instances use the PostgreSQL instance registry.
// 	useInstanceRegistry := s.Pool != nil
// 	// useInstanceRegistry := s.Pool != nil && s.RunOnceFunc == nil

// 	pollInterval := s.PollInterval
// 	if pollInterval <= 0 {
// 		pollInterval = 500 * time.Millisecond
// 	}

// 	leaseDuration := s.LeaseDuration
// 	if leaseDuration <= 0 {
// 		leaseDuration = 30 * time.Second
// 	}

// 	heartbeatInterval := s.HeartbeatInterval
// 	if heartbeatInterval <= 0 {
// 		heartbeatInterval = leaseDuration / 3
// 	}

// 	if heartbeatInterval <= 0 || heartbeatInterval >= leaseDuration {
// 		return errors.New(
// 			"scheduler heartbeat interval must be positive and shorter than lease duration",
// 		)
// 	}

// 	if useInstanceRegistry {
// 		if s.InstanceID == "" || s.Hostname == "" {
// 			return errors.New("scheduler instance ID and hostname are required")
// 		}

// 		if err := database.RegisterSchedulerInstance(
// 			ctx,
// 			s.Pool,
// 			s.InstanceID,
// 			s.Hostname,
// 			leaseDuration,
// 		); err != nil {
// 			return fmt.Errorf("register scheduler instance: %w", err)
// 		}

// 		defer func() {
// 			stopCtx, cancel := context.WithTimeout(
// 				context.Background(),
// 				5*time.Second,
// 			)
// 			defer cancel()

// 			if err := database.StopSchedulerInstance(
// 				stopCtx,
// 				s.Pool,
// 				s.InstanceID,
// 			); err != nil {
// 				fmt.Printf(
// 					"stop scheduler instance: %v\n",
// 					err,
// 				)
// 			}
// 		}()
// 	}

// 	pollTicker := time.NewTicker(pollInterval)
// 	defer pollTicker.Stop()

// 	heartbeatTicker := time.NewTicker(heartbeatInterval)
// 	defer heartbeatTicker.Stop()

// 	for {
// 		select {
// 		case <-ctx.Done():
// 			return ctx.Err()

// 		case <-heartbeatTicker.C:
// 			if !useInstanceRegistry {
// 				continue
// 			}

// 			if err := database.RenewSchedulerInstanceLease(
// 				ctx,
// 				s.Pool,
// 				s.InstanceID,
// 				leaseDuration,
// 			); err != nil {
// 				return fmt.Errorf(
// 					"renew scheduler lease: %w",
// 					err,
// 				)
// 			}

// 		case <-pollTicker.C:
// 			runOnce := s.RunOnce

// 			if s.RunOnceFunc != nil {
// 				runOnce = s.RunOnceFunc
// 			} else if s.Pool == nil {
// 				// No database pool: wait for cancellation in test mode.
// 				continue
// 			}

// 			if err := runOnce(ctx); err != nil {
// 				return fmt.Errorf("scheduler dispatch: %w", err)
// 			}
// 		}
// 	}
// }

// func (s *Scheduler) RunOnce(ctx context.Context) error {
// 	if s.Pool == nil {
// 		return errors.New("scheduler database pool is nil")
// 	}

// 	_, err := database.ClaimTaskAndCreateJob(ctx, s.Pool)
// 	return err
// }
