package database

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/joho/godotenv"
)

func TestSchedulerLeadershipLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		20*time.Second,
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

	instanceA := fmt.Sprintf(
		"leader-test-a-%d",
		time.Now().UnixNano(),
	)
	instanceB := fmt.Sprintf(
		"leader-test-b-%d",
		time.Now().UnixNano(),
	)

	const leaseDuration = 30 * time.Second

	for _, id := range []string{instanceA, instanceB} {
		if err := RegisterSchedulerInstance(
			ctx,
			pool,
			id,
			"leadership-test-host",
			leaseDuration,
		); err != nil {
			t.Fatalf("register scheduler %s: %v", id, err)
		}
	}

	defer func() {
		_, _ = pool.Exec(
			context.Background(),
			`
			UPDATE scheduler_leadership
			SET instance_id = NULL, lease_until = NULL
			WHERE instance_id = ANY($1)
			`,
			[]string{instanceA, instanceB},
		)

		_, _ = pool.Exec(
			context.Background(),
			`DELETE FROM scheduler_instances WHERE id = ANY($1)`,
			[]string{instanceA, instanceB},
		)
	}()

	// Step 1: Scheduler A acquires leadership.
	leadershipA, err := AcquireSchedulerLeadership(
		ctx,
		pool,
		instanceA,
		leaseDuration,
	)
	if err != nil {
		t.Fatalf("scheduler A acquire leadership: %v", err)
	}

	if leadershipA.InstanceID != instanceA {
		t.Fatalf(
			"expected leader %q, got %q",
			instanceA,
			leadershipA.InstanceID,
		)
	}

	if leadershipA.FencingToken <= 0 {
		t.Fatalf(
			"expected positive fencing token, got %d",
			leadershipA.FencingToken,
		)
	}

	// Step 2: Scheduler B must not acquire active leadership.
	_, err = AcquireSchedulerLeadership(
		ctx,
		pool,
		instanceB,
		leaseDuration,
	)

	if !errors.Is(err, ErrSchedulerLeadershipNotAcquired) {
		t.Fatalf(
			"expected leadership acquisition rejection, got %v",
			err,
		)
	}

	// Step 3: Scheduler A renews leadership.
	err = RenewSchedulerLeadership(
		ctx,
		pool,
		instanceA,
		leadershipA.FencingToken,
		leaseDuration,
	)
	if err != nil {
		t.Fatalf("scheduler A renew leadership: %v", err)
	}

	// Step 4: Expire A's leadership lease.
	_, err = pool.Exec(
		ctx,
		`
		UPDATE scheduler_leadership
		SET lease_until = NOW() - INTERVAL '1 second'
		WHERE singleton_id = 1
		`,
	)
	if err != nil {
		t.Fatalf("expire leadership lease: %v", err)
	}

	// Step 5: Scheduler B takes over after expiration.
	leadershipB, err := AcquireSchedulerLeadership(
		ctx,
		pool,
		instanceB,
		leaseDuration,
	)
	if err != nil {
		t.Fatalf("scheduler B acquire leadership: %v", err)
	}

	if leadershipB.InstanceID != instanceB {
		t.Fatalf(
			"expected leader %q, got %q",
			instanceB,
			leadershipB.InstanceID,
		)
	}

	if leadershipB.FencingToken <= leadershipA.FencingToken {
		t.Fatalf(
			"expected fencing token to increase: A=%d B=%d",
			leadershipA.FencingToken,
			leadershipB.FencingToken,
		)
	}

	// Step 6: Old scheduler A must not renew leadership.
	err = RenewSchedulerLeadership(
		ctx,
		pool,
		instanceA,
		leadershipA.FencingToken,
		leaseDuration,
	)

	if !errors.Is(err, ErrSchedulerLeaseLost) {
		t.Fatalf(
			"expected old leader renewal rejection, got %v",
			err,
		)
	}

	// Step 7: Old scheduler A must not release B's leadership.
	err = ReleaseSchedulerLeadership(
		ctx,
		pool,
		instanceA,
		leadershipA.FencingToken,
	)

	if !errors.Is(err, ErrSchedulerLeadershipNotAcquired) {
		t.Fatalf(
			"expected old leader release rejection, got %v",
			err,
		)
	}

	// Step 8: Current leader B releases leadership.
	err = ReleaseSchedulerLeadership(
		ctx,
		pool,
		instanceB,
		leadershipB.FencingToken,
	)
	if err != nil {
		t.Fatalf("scheduler B release leadership: %v", err)
	}

	// Step 9: Verify leadership is released.
	var (
		currentInstance *string
		currentLease    *time.Time
		currentToken    int64
	)

	err = pool.QueryRow(
		ctx,
		`
		SELECT instance_id, lease_until, fencing_token
		FROM scheduler_leadership
		WHERE singleton_id = 1
		`,
	).Scan(
		&currentInstance,
		&currentLease,
		&currentToken,
	)
	if err != nil {
		t.Fatalf("read released leadership: %v", err)
	}

	if currentInstance != nil {
		t.Fatalf(
			"expected no leader, got %q",
			*currentInstance,
		)
	}

	if currentLease != nil {
		t.Fatal("expected released leadership lease to be NULL")
	}

	if currentToken != leadershipB.FencingToken {
		t.Fatalf(
			"expected fencing token to remain %d, got %d",
			leadershipB.FencingToken,
			currentToken,
		)
	}
}

func TestConcurrentSchedulerLeadershipAcquisition(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		20*time.Second,
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

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())

	instanceA := "concurrent-leader-a-" + suffix
	instanceB := "concurrent-leader-b-" + suffix

	const leaseDuration = 30 * time.Second

	for _, id := range []string{instanceA, instanceB} {
		if err := RegisterSchedulerInstance(
			ctx,
			pool,
			id,
			"concurrent-test-host",
			leaseDuration,
		); err != nil {
			t.Fatalf("register scheduler %s: %v", id, err)
		}
	}

	defer func() {
		_, _ = pool.Exec(
			context.Background(),
			`
			UPDATE scheduler_leadership
			SET instance_id = NULL, lease_until = NULL
			WHERE instance_id = ANY($1)
			`,
			[]string{instanceA, instanceB},
		)

		_, _ = pool.Exec(
			context.Background(),
			`DELETE FROM scheduler_instances WHERE id = ANY($1)`,
			[]string{instanceA, instanceB},
		)
	}()

	type result struct {
		instanceID string
		leadership SchedulerLeadership
		err        error
	}

	start := make(chan struct{})
	results := make(chan result, 2)

	var wg sync.WaitGroup

	for _, id := range []string{instanceA, instanceB} {
		wg.Add(1)

		go func(instanceID string) {
			defer wg.Done()

			<-start

			leadership, err := AcquireSchedulerLeadership(
				ctx,
				pool,
				instanceID,
				leaseDuration,
			)

			results <- result{
				instanceID: instanceID,
				leadership: leadership,
				err:        err,
			}
		}(id)
	}

	// Release both goroutines at nearly the same time.
	close(start)

	wg.Wait()
	close(results)

	var (
		successCount int
		failureCount int
		winner       string
	)

	for result := range results {
		if result.err == nil {
			successCount++
			winner = result.instanceID

			if result.leadership.InstanceID != result.instanceID {
				t.Fatalf(
					"expected leader %q, got %q",
					result.instanceID,
					result.leadership.InstanceID,
				)
			}

			continue
		}

		if errors.Is(
			result.err,
			ErrSchedulerLeadershipNotAcquired,
		) {
			failureCount++
			continue
		}

		t.Fatalf(
			"unexpected acquisition error for %s: %v",
			result.instanceID,
			result.err,
		)
	}

	if successCount != 1 {
		t.Fatalf(
			"expected exactly one successful leader, got %d",
			successCount,
		)
	}

	if failureCount != 1 {
		t.Fatalf(
			"expected exactly one rejected scheduler, got %d",
			failureCount,
		)
	}

	t.Logf("scheduler %s won leadership", winner)
}

func TestSchedulerLeadershipRejectsInactiveInstances(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		20*time.Second,
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

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())

	expiredID := "expired-scheduler-" + suffix
	stoppedID := "stopped-scheduler-" + suffix

	const leaseDuration = 30 * time.Second

	// Avoid disturbing a leader belonging to another running process.
	var leadershipAvailable bool
	err = pool.QueryRow(
		ctx,
		`
		SELECT
			instance_id IS NULL OR lease_until <= NOW()
		FROM scheduler_leadership
		WHERE singleton_id = 1
		`,
	).Scan(&leadershipAvailable)
	if err != nil {
		t.Fatalf("check current leadership: %v", err)
	}

	if !leadershipAvailable {
		t.Skip("another scheduler currently owns active leadership")
	}

	for _, id := range []string{expiredID, stoppedID} {
		if err := RegisterSchedulerInstance(
			ctx,
			pool,
			id,
			"inactive-test-host",
			leaseDuration,
		); err != nil {
			t.Fatalf("register scheduler %s: %v", id, err)
		}
	}

	defer func() {
		_, _ = pool.Exec(
			context.Background(),
			`
			UPDATE scheduler_leadership
			SET instance_id = NULL, lease_until = NULL
			WHERE instance_id = ANY($1)
			`,
			[]string{expiredID, stoppedID},
		)

		_, _ = pool.Exec(
			context.Background(),
			`DELETE FROM scheduler_instances WHERE id = ANY($1)`,
			[]string{expiredID, stoppedID},
		)
	}()

	// Expire the first scheduler's instance lease.
	_, err = pool.Exec(
		ctx,
		`
		UPDATE scheduler_instances
		SET lease_until = NOW() - INTERVAL '1 second'
		WHERE id = $1
		`,
		expiredID,
	)
	if err != nil {
		t.Fatalf("expire scheduler instance: %v", err)
	}

	// Stop the second scheduler.
	if err := StopSchedulerInstance(ctx, pool, stoppedID); err != nil {
		t.Fatalf("stop scheduler instance: %v", err)
	}

	// An expired instance must not acquire leadership.
	_, err = AcquireSchedulerLeadership(
		ctx,
		pool,
		expiredID,
		leaseDuration,
	)
	if !errors.Is(err, ErrSchedulerLeadershipNotAcquired) {
		t.Fatalf(
			"expected expired scheduler rejection, got %v",
			err,
		)
	}

	// A stopped instance must not acquire leadership.
	_, err = AcquireSchedulerLeadership(
		ctx,
		pool,
		stoppedID,
		leaseDuration,
	)
	if !errors.Is(err, ErrSchedulerLeadershipNotAcquired) {
		t.Fatalf(
			"expected stopped scheduler rejection, got %v",
			err,
		)
	}
}
