package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"chronos/database"

	"github.com/joho/godotenv"
)

func TestSchedulerTakesOverAfterLeaderInstanceLeaseExpires(
	t *testing.T,
) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		25*time.Second,
	)
	defer cancel()

	if err := godotenv.Load("../.env"); err != nil {
		t.Fatalf("load .env: %v", err)
	}

	pool, err := database.Connect(ctx)
	if err != nil {
		t.Fatalf("connect database: %v", err)
	}

	instanceA := fmt.Sprintf("lease-loss-a-%d", time.Now().UnixNano())
	instanceB := fmt.Sprintf("lease-loss-b-%d", time.Now().UnixNano())

	var leadershipAvailable bool

	err = pool.QueryRow(
		ctx,
		`
		SELECT instance_id IS NULL
		    OR lease_until <= clock_timestamp()
		FROM scheduler_leadership
		WHERE singleton_id = 1
		`,
	).Scan(&leadershipAvailable)

	if err != nil {
		pool.Close()
		t.Fatalf("check leadership availability: %v", err)
	}

	if !leadershipAvailable {
		pool.Close()
		t.Skip("another scheduler currently owns active leadership")
	}

	var dispatchA atomic.Int64
	var dispatchB atomic.Int64

	const (
		leaseDuration     = 1200 * time.Millisecond
		heartbeatInterval = 200 * time.Millisecond
		pollInterval      = 25 * time.Millisecond
	)

	schedulerA := New(pool)
	schedulerA.InstanceID = instanceA
	schedulerA.Hostname = "lease-loss-host-a"
	schedulerA.LeaseDuration = leaseDuration
	schedulerA.HeartbeatInterval = heartbeatInterval
	schedulerA.PollInterval = pollInterval
	schedulerA.RunOnceFunc = func(ctx context.Context) error {
		dispatchA.Add(1)
		return nil
	}

	schedulerB := New(pool)
	schedulerB.InstanceID = instanceB
	schedulerB.Hostname = "lease-loss-host-b"
	schedulerB.LeaseDuration = leaseDuration
	schedulerB.HeartbeatInterval = heartbeatInterval
	schedulerB.PollInterval = pollInterval
	schedulerB.RunOnceFunc = func(ctx context.Context) error {
		dispatchB.Add(1)
		return nil
	}

	ctxA, cancelA := context.WithCancel(ctx)
	ctxB, cancelB := context.WithCancel(ctx)

	doneA := make(chan error, 1)
	doneB := make(chan error, 1)

	go func() {
		doneA <- schedulerA.Run(ctxA)
	}()

	go func() {
		doneB <- schedulerB.Run(ctxB)
	}()

	defer func() {
		cancelA()
		cancelB()

		<-doneA
		<-doneB

		_, _ = pool.Exec(
			context.Background(),
			`
			UPDATE scheduler_leadership
			SET instance_id = NULL,
			    lease_until = NULL,
			    updated_at = NOW()
			WHERE singleton_id = 1
			  AND instance_id = ANY($1::text[])
			`,
			[]string{instanceA, instanceB},
		)

		_, _ = pool.Exec(
			context.Background(),
			`
			DELETE FROM scheduler_instances
			WHERE id = ANY($1::text[])
			`,
			[]string{instanceA, instanceB},
		)

		pool.Close()
	}()

	// Wait until one scheduler starts dispatching.
	deadline := time.Now().Add(5 * time.Second)

	for dispatchA.Load() == 0 && dispatchB.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no scheduler acquired leadership")
		}

		time.Sleep(10 * time.Millisecond)
	}

	time.Sleep(250 * time.Millisecond)

	if dispatchA.Load() > 0 && dispatchB.Load() > 0 {
		t.Fatalf(
			"both schedulers dispatched: A=%d B=%d",
			dispatchA.Load(),
			dispatchB.Load(),
		)
	}

	// Read the actual leader and its fencing token.
	var oldLeaderID string
	var oldFencingToken int64

	err = pool.QueryRow(
		ctx,
		`
		SELECT instance_id, fencing_token
		FROM scheduler_leadership
		WHERE singleton_id = 1
		`,
	).Scan(&oldLeaderID, &oldFencingToken)

	if err != nil {
		t.Fatalf("read current leadership: %v", err)
	}

	if oldLeaderID != instanceA && oldLeaderID != instanceB {
		t.Fatalf("unexpected leader: %s", oldLeaderID)
	}

	t.Logf(
		"current leader: %s, fencing token: %d",
		oldLeaderID,
		oldFencingToken,
	)

	// Simulate an unexpected instance lease expiration.
	_, err = pool.Exec(
		ctx,
		`
		UPDATE scheduler_instances
		SET lease_until = clock_timestamp() - INTERVAL '1 second'
		WHERE id = $1
		`,
		oldLeaderID,
	)

	if err != nil {
		t.Fatalf("expire leader instance lease: %v", err)
	}

	// Wait until the old leader detects the lost instance lease
	// and completes its shutdown.
	deadline = time.Now().Add(5 * time.Second)

	for {
		var status string

		err = pool.QueryRow(
			ctx,
			`
			SELECT status
			FROM scheduler_instances
			WHERE id = $1
			`,
			oldLeaderID,
		).Scan(&status)

		if err != nil {
			t.Fatalf("read old leader status: %v", err)
		}

		if status == "stopped" {
			break
		}

		if time.Now().After(deadline) {
			t.Fatal("old leader did not stop after lease expiration")
		}

		time.Sleep(20 * time.Millisecond)
	}

	// Determine which scheduler was the standby.
	standbyID := instanceA
	standbyCount := &dispatchA

	if oldLeaderID == instanceA {
		standbyID = instanceB
		standbyCount = &dispatchB
	}

	// Wait for the standby to start dispatching.
	deadline = time.Now().Add(5 * time.Second)

	for standbyCount.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatalf(
				"standby %s did not take over",
				standbyID,
			)
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Logf("standby %s successfully took over", standbyID)

	// The old leader's fencing token must no longer authorize dispatch.
	_, err = database.ClaimTaskAndCreateJobAsLeader(
		ctx,
		pool,
		oldLeaderID,
		oldFencingToken,
	)

	if !errors.Is(err, database.ErrSchedulerLeaseLost) {
		t.Fatalf(
			"expected old leader token to be rejected, got: %v",
			err,
		)
	}

	t.Log("old leader fencing token was correctly rejected")
}
