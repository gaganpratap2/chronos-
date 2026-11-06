package scheduler

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"chronos/database"

	"github.com/joho/godotenv"
)

// two-scheduler integration test

func TestTwoSchedulersOnlyLeaderDispatchesAndStandbyTakesOver(
	t *testing.T,
) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		30*time.Second,
	)
	defer cancel()

	if err := godotenv.Load("../.env"); err != nil {
		t.Fatalf("load .env: %v", err)
	}

	pool, err := database.Connect(ctx)
	if err != nil {
		t.Fatalf("connect database: %v", err)
	}

	instanceA := fmt.Sprintf("ha-test-a-%d", time.Now().UnixNano())
	instanceB := fmt.Sprintf("ha-test-b-%d", time.Now().UnixNano())

	// Do not interfere with a scheduler running outside this test.
	var leadershipAvailable bool

	err = pool.QueryRow(
		ctx,
		`
		SELECT
			instance_id IS NULL
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
		leaseDuration     = 900 * time.Millisecond
		heartbeatInterval = 150 * time.Millisecond
		pollInterval      = 25 * time.Millisecond
	)

	schedulerA := New(pool)
	schedulerA.InstanceID = instanceA
	schedulerA.Hostname = "ha-test-host-a"
	schedulerA.LeaseDuration = leaseDuration
	schedulerA.HeartbeatInterval = heartbeatInterval
	schedulerA.PollInterval = pollInterval
	schedulerA.RunOnceFunc = func(ctx context.Context) error {
		dispatchA.Add(1)
		return nil
	}

	schedulerB := New(pool)
	schedulerB.InstanceID = instanceB
	schedulerB.Hostname = "ha-test-host-b"
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

	defer func() {
		cancelA()
		cancelB()

		<-doneA
		<-doneB

		pool.Close()
	}()

	go func() {
		doneA <- schedulerA.Run(ctxA)
	}()

	go func() {
		doneB <- schedulerB.Run(ctxB)
	}()

	// Wait until one scheduler starts dispatching.
	waitForDispatch := func(
		name string,
		count *atomic.Int64,
		done <-chan error,
	) {
		t.Helper()

		deadline := time.After(5 * time.Second)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()

		for {
			if count.Load() > 0 {
				return
			}

			select {
			case runErr := <-done:
				t.Fatalf(
					"%s stopped before dispatching: %v",
					name,
					runErr,
				)
			case <-deadline:
				t.Fatalf(
					"timed out waiting for %s to dispatch",
					name,
				)
			case <-ticker.C:
			}
		}
	}

	if dispatchA.Load() == 0 && dispatchB.Load() == 0 {
		// Either scheduler may win the initial election.
		deadline := time.After(5 * time.Second)
		ticker := time.NewTicker(10 * time.Millisecond)

		for dispatchA.Load() == 0 && dispatchB.Load() == 0 {
			select {
			case runErr := <-doneA:
				t.Fatalf("scheduler A stopped unexpectedly: %v", runErr)
			case runErr := <-doneB:
				t.Fatalf("scheduler B stopped unexpectedly: %v", runErr)
			case <-deadline:
				t.Fatal("no scheduler acquired leadership")
			case <-ticker.C:
			}
		}

		ticker.Stop()
	}

	// Give both schedulers time to run several polling cycles.
	time.Sleep(300 * time.Millisecond)

	countA := dispatchA.Load()
	countB := dispatchB.Load()

	if countA > 0 && countB > 0 {
		t.Fatalf(
			"both schedulers dispatched simultaneously: A=%d B=%d",
			countA,
			countB,
		)
	}

	var leaderCancel context.CancelFunc
	var standbyCount *atomic.Int64
	var standbyDone <-chan error
	var leaderName string
	var standbyName string

	if countA > 0 {
		leaderCancel = cancelA
		standbyCount = &dispatchB
		standbyDone = doneB
		leaderName = "A"
		standbyName = "B"
	} else {
		leaderCancel = cancelB
		standbyCount = &dispatchA
		standbyDone = doneA
		leaderName = "B"
		standbyName = "A"
	}

	t.Logf(
		"scheduler %s was leader; scheduler %s remained standby",
		leaderName,
		standbyName,
	)

	// Stop the current leader. Its deferred cleanup releases leadership.
	leaderCancel()

	// The standby should acquire leadership and begin dispatching.
	waitForDispatch(
		standbyName,
		standbyCount,
		standbyDone,
	)

	t.Logf(
		"scheduler %s successfully took over leadership",
		standbyName,
	)
}
