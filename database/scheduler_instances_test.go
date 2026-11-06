package database

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/joho/godotenv"
)

func TestSchedulerInstanceLeaseLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		15*time.Second,
	)
	defer cancel()

	if err := godotenv.Load("../.env"); err != nil {
		t.Fatalf("load .env: %v", err)
	}

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("connect database: %v", err)
	}
	defer pool.Close()

	instanceID := fmt.Sprintf(
		"scheduler-test-%d",
		time.Now().UnixNano(),
	)

	defer func() {
		_, _ = pool.Exec(
			context.Background(),
			`DELETE FROM scheduler_instances WHERE id = $1`,
			instanceID,
		)
	}()

	const (
		hostname      = "test-host"
		leaseDuration = 30 * time.Second
	)

	// Step 1: Register a scheduler.
	err = RegisterSchedulerInstance(
		ctx,
		pool,
		instanceID,
		hostname,
		leaseDuration,
	)
	if err != nil {
		t.Fatalf("register scheduler: %v", err)
	}

	var (
		status string
		dbHost string
		lease  time.Time
	)

	err = pool.QueryRow(
		ctx,
		`
		SELECT status, hostname, lease_until
		FROM scheduler_instances
		WHERE id = $1
		`,
		instanceID,
	).Scan(&status, &dbHost, &lease)

	if err != nil {
		t.Fatalf("read scheduler: %v", err)
	}

	if status != "running" {
		t.Fatalf("expected running status, got %q", status)
	}

	if dbHost != hostname {
		t.Fatalf("expected hostname %q, got %q", hostname, dbHost)
	}

	if !lease.After(time.Now()) {
		t.Fatal("expected lease to be in the future")
	}

	// Step 2: Renew the scheduler lease.
	err = RenewSchedulerInstanceLease(
		ctx,
		pool,
		instanceID,
		leaseDuration,
	)
	if err != nil {
		t.Fatalf("renew scheduler lease: %v", err)
	}

	// Step 3: Force the lease to expire.
	_, err = pool.Exec(
		ctx,
		`
		UPDATE scheduler_instances
		SET lease_until = NOW() - INTERVAL '1 second'
		WHERE id = $1
		`,
		instanceID,
	)
	if err != nil {
		t.Fatalf("expire scheduler lease: %v", err)
	}

	// An expired scheduler must not renew its old lease.
	err = RenewSchedulerInstanceLease(
		ctx,
		pool,
		instanceID,
		leaseDuration,
	)

	if !errors.Is(err, ErrSchedulerLeaseLost) {
		t.Fatalf(
			"expected ErrSchedulerLeaseLost, got %v",
			err,
		)
	}

	// Step 4: Stop the scheduler.
	err = StopSchedulerInstance(
		ctx,
		pool,
		instanceID,
	)
	if err != nil {
		t.Fatalf("stop scheduler: %v", err)
	}

	err = pool.QueryRow(
		ctx,
		`
		SELECT status
		FROM scheduler_instances
		WHERE id = $1
		`,
		instanceID,
	).Scan(&status)

	if err != nil {
		t.Fatalf("read stopped scheduler: %v", err)
	}

	if status != "stopped" {
		t.Fatalf("expected stopped status, got %q", status)
	}

	// A stopped scheduler must not renew its lease.
	err = RenewSchedulerInstanceLease(
		ctx,
		pool,
		instanceID,
		leaseDuration,
	)

	if !errors.Is(err, ErrSchedulerLeaseLost) {
		t.Fatalf(
			"expected ErrSchedulerLeaseLost for stopped scheduler, got %v",
			err,
		)
	}
}
