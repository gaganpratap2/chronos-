package database

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrSchedulerLeadershipNotAcquired = errors.New(
	"scheduler leadership was not acquired",
)

type SchedulerLeadership struct {
	InstanceID   string
	FencingToken int64
	LeaseUntil   time.Time
}

func AcquireSchedulerLeadership(
	ctx context.Context,
	pool *pgxpool.Pool,
	instanceID string,
	leaseDuration time.Duration,
) (SchedulerLeadership, error) {
	if pool == nil {
		return SchedulerLeadership{}, errors.New("database pool is nil")
	}

	if strings.TrimSpace(instanceID) == "" {
		return SchedulerLeadership{}, errors.New("scheduler instance ID is empty")
	}

	if leaseDuration < time.Millisecond {
		return SchedulerLeadership{}, errors.New(
			"scheduler lease duration must be at least 1ms",
		)
	}

	var leadership SchedulerLeadership

	err := pool.QueryRow(
		ctx,
		`
		UPDATE scheduler_leadership AS leadership
		SET
			instance_id = instance.id,
			fencing_token = leadership.fencing_token + 1,
			lease_until = NOW() + ($2 * INTERVAL '1 millisecond'),
			updated_at = NOW()
		FROM scheduler_instances AS instance
		WHERE leadership.singleton_id = 1
			AND instance.id = $1
			AND instance.status = 'running'
			AND instance.lease_until > NOW()
			AND (
				leadership.instance_id IS NULL
				OR leadership.lease_until <= NOW()
			)
		RETURNING
			leadership.instance_id,
			leadership.fencing_token,
			leadership.lease_until
		`,
		instanceID,
		leaseDuration.Milliseconds(),
	).Scan(
		&leadership.InstanceID,
		&leadership.FencingToken,
		&leadership.LeaseUntil,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return SchedulerLeadership{}, ErrSchedulerLeadershipNotAcquired
	}

	if err != nil {
		return SchedulerLeadership{}, fmt.Errorf(
			"acquire scheduler leadership: %w",
			err,
		)
	}

	return leadership, nil
}

/*
Scheduler A acquires leadership with token 1.

A's lease expires.

Scheduler B acquires leadership with token 2.

A wakes up and attempts renewal using token 1.
*/

func RenewSchedulerLeadership(
	ctx context.Context,
	pool *pgxpool.Pool,
	instanceID string,
	fencingToken int64,
	leaseDuration time.Duration,
) error {
	if pool == nil {
		return errors.New("database pool is nil")
	}

	if strings.TrimSpace(instanceID) == "" {
		return errors.New("scheduler instance ID is empty")
	}

	if fencingToken <= 0 {
		return errors.New("scheduler fencing token must be positive")
	}

	if leaseDuration < time.Millisecond {
		return errors.New(
			"scheduler lease duration must be at least 1ms",
		)
	}

	tag, err := pool.Exec(
		ctx,
		`
		UPDATE scheduler_leadership AS leadership
		SET
			lease_until = NOW() + ($3 * INTERVAL '1 millisecond'),
			updated_at = NOW()
		FROM scheduler_instances AS instance
		WHERE leadership.singleton_id = 1
			AND leadership.instance_id = $1
			AND leadership.fencing_token = $2
			AND leadership.lease_until > NOW()
			AND instance.id = $1
			AND instance.status = 'running'
			AND instance.lease_until > NOW()
		`,
		instanceID,
		fencingToken,
		leaseDuration.Milliseconds(),
	)
	if err != nil {
		return fmt.Errorf(
			"renew scheduler leadership: %w",
			err,
		)
	}

	if tag.RowsAffected() != 1 {
		return ErrSchedulerLeaseLost
	}

	return nil
}

func ReleaseSchedulerLeadership(
	ctx context.Context,
	pool *pgxpool.Pool,
	instanceID string,
	fencingToken int64,
) error {
	if pool == nil {
		return errors.New("database pool is nil")
	}

	if strings.TrimSpace(instanceID) == "" {
		return errors.New("scheduler instance ID is empty")
	}

	if fencingToken <= 0 {
		return errors.New("scheduler fencing token must be positive")
	}

	tag, err := pool.Exec(
		ctx,
		`
		UPDATE scheduler_leadership
		SET
			instance_id = NULL,
			lease_until = NULL,
			updated_at = NOW()
		WHERE singleton_id = 1
			AND instance_id = $1
			AND fencing_token = $2
		`,
		instanceID,
		fencingToken,
	)
	if err != nil {
		return fmt.Errorf(
			"release scheduler leadership: %w",
			err,
		)
	}

	if tag.RowsAffected() != 1 {
		return ErrSchedulerLeadershipNotAcquired
	}

	return nil
}
