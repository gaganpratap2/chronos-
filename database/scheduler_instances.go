package database

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrSchedulerLeaseLost = errors.New(
	"scheduler lease is lost or expired",
)

func RegisterSchedulerInstance(ctx context.Context, pool *pgxpool.Pool, instanceID string, hostname string, leaseDuration time.Duration) error {

	if pool == nil {
		return errors.New("database pool is nil")
	}

	if strings.TrimSpace(instanceID) == "" {
		return errors.New("scheduler instance ID is empty")
	}

	if strings.TrimSpace(hostname) == "" {
		return errors.New("scheduler hostname is empty")
	}

	if leaseDuration < time.Millisecond {
		return errors.New("scheduler lease duration must be at least 1ms")
	}

	_, err := pool.Exec(
		ctx,
		`INSERT INTO scheduler_instances(
			id ,
			hostname,
			lease_until
		)
		VALUES ($1 , $2 , NOW() + ($3 * INTERVAL '1 millisecond'))
		`,
		instanceID, hostname, leaseDuration.Milliseconds(),
	)

	if err != nil {
		return fmt.Errorf(
			"register scheduler instance: %w", err,
		)
	}

	return nil
}

func RenewSchedulerInstanceLease(ctx context.Context, pool *pgxpool.Pool, instanceID string, leaseDuration time.Duration) error {

	if pool == nil {
		return errors.New("database pool is nil")
	}

	if strings.TrimSpace(instanceID) == "" {
		return errors.New("scheduler instance ID is empty")
	}

	if leaseDuration < time.Millisecond {
		return errors.New("scheduler lease duration must be at least 1ms")
	}

	tag, err := pool.Exec(ctx,
		`UPDATE scheduler_instances
	SET 
		last_heartbeat_at = NOW(),
		lease_until = NOW() + ($2 * INTERVAL '1 millisecond')
	WHERE id = $1
		AND status = 'running'
		AND lease_until > NOW()
	`,
		instanceID,
		leaseDuration.Milliseconds(),
	)

	if err != nil {
		return fmt.Errorf(
			"renew scheduler lease: %w", err,
		)
	}

	if tag.RowsAffected() != 1 {
		return ErrSchedulerLeaseLost
	}
	return nil

}

func StopSchedulerInstance(ctx context.Context, pool *pgxpool.Pool, instanceID string) error {

	if pool == nil {
		return errors.New("database pool is nil")
	}

	if strings.TrimSpace(instanceID) == "" {
		return errors.New("scheduler instance ID is empty")
	}

	_, err := pool.Exec(ctx,
		`UPDATE scheduler_instances
	SET 
		status = 'stopped',
		stopped_at = NOW(),
		lease_until = NOW()
	WHERE id = $1
		AND status = 'running'
	`, instanceID,
	)

	if err != nil {
		return fmt.Errorf(
			"stop scheduler instance: %w", err,
		)
	}

	return nil
}
