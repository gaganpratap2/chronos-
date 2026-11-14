package scheduler

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"chronos/database"

	"github.com/joho/godotenv"
)

func TestSchedulerInstanceRuntimeLifecycle(t *testing.T) {
	// Load the project environment variables.
	_ = godotenv.Load("../.env")

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	pool, err := database.Connect(ctx)
	if err != nil {
		t.Fatalf("connect to database: %v", err)
	}
	defer pool.Close()

	instanceID := fmt.Sprintf(
		"scheduler-runtime-test-%d",
		time.Now().UnixNano(),
	)

	// Clean up the test instance even if an assertion fails.
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cleanupCancel()

		_, _ = pool.Exec(
			cleanupCtx,
			`DELETE FROM scheduler_instances WHERE id = $1`,
			instanceID,
		)
	}()

	s := &Scheduler{
		Pool:              pool,
		InstanceID:        instanceID,
		Hostname:          "scheduler-test-host",
		PollInterval:      time.Hour,
		LeaseDuration:     5 * time.Second,
		HeartbeatInterval: 250 * time.Millisecond,

		// Prevent real workflow task dispatch.
		RunOnceFunc: func(context.Context) error {
			return nil
		},
	}

	runCtx, stop := context.WithCancel(ctx)
	defer stop()

	done := make(chan error, 1)

	go func() {
		done <- s.Run(runCtx)
	}()

	// Step 1: Wait for registration.
	var initialHeartbeat time.Time

	deadline := time.Now().Add(3 * time.Second)

	for time.Now().Before(deadline) {
		err = pool.QueryRow(
			ctx,
			`SELECT last_heartbeat_at
			 FROM scheduler_instances
			 WHERE id = $1 AND status = 'running'`,
			instanceID,
		).Scan(&initialHeartbeat)

		if err == nil {
			break
		}

		time.Sleep(20 * time.Millisecond)
	}

	if err != nil {
		t.Fatalf("scheduler instance was not registered: %v", err)
	}

	// Step 2: Wait for at least one heartbeat renewal.
	// Step 2: Wait until at least one heartbeat renewal is recorded.
	var renewedHeartbeat time.Time
	var leaseUntil time.Time

	heartbeatDeadline := time.Now().Add(3 * time.Second)

	for time.Now().Before(heartbeatDeadline) {
		err = pool.QueryRow(
			ctx,
			`SELECT last_heartbeat_at, lease_until
		 FROM scheduler_instances
		 WHERE id = $1 AND status = 'running'`,
			instanceID,
		).Scan(&renewedHeartbeat, &leaseUntil)

		if err == nil && renewedHeartbeat.After(initialHeartbeat) {
			break
		}

		select {
		case <-ctx.Done():
			t.Fatalf("context ended while waiting for heartbeat: %v", ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}

	if err != nil {
		t.Fatalf("read renewed scheduler lease: %v", err)
	}

	if !renewedHeartbeat.After(initialHeartbeat) {
		t.Fatal("scheduler heartbeat was not renewed")
	}

	if !renewedHeartbeat.After(initialHeartbeat) {
		t.Fatal("scheduler heartbeat was not renewed")
	}

	if !leaseUntil.After(time.Now()) {
		t.Fatal("scheduler lease has expired")
	}

	// Step 3: Cancel the scheduler.
	stop()

	select {
	case runErr := <-done:
		if !errors.Is(runErr, context.Canceled) {
			t.Fatalf(
				"expected context.Canceled, got %v",
				runErr,
			)
		}

	case <-time.After(3 * time.Second):
		t.Fatal("scheduler did not stop after cancellation")
	}

	// Step 4: Verify graceful shutdown updated the database.
	var status string

	err = pool.QueryRow(
		ctx,
		`SELECT status
		 FROM scheduler_instances
		 WHERE id = $1`,
		instanceID,
	).Scan(&status)

	if err != nil {
		t.Fatalf("read scheduler status after shutdown: %v", err)
	}

	if status != "stopped" {
		t.Fatalf(
			"expected scheduler status stopped, got %q",
			status,
		)
	}
}
