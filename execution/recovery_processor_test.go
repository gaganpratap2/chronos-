package execution

import (
	"errors"
	"testing"
	"time"

	"chronos/database"
)

func TestRecoveryProcessorReconcilesSuccess(t *testing.T) {
	f := newServiceFixture(t)

	request := f.request(
		"charge-payment",
		"recovery-processor-success",
		"payment-provider",
		[]byte(`{"amount":1000,"currency":"INR"}`),
	)

	operation, created, err := database.ReserveExecutionOperation(
		f.ctx,
		f.pool,
		f.databaseInput(request),
	)
	if err != nil {
		t.Fatalf("reserve operation: %v", err)
	}
	if !created {
		t.Fatal("expected operation to be created")
	}

	if err := database.MarkExecutionOutcomeUnknown(
		f.ctx,
		f.pool,
		operation.ID,
		f.fencingToken,
		"provider response timed out",
	); err != nil {
		t.Fatalf("mark outcome unknown: %v", err)
	}

	_, err = f.pool.Exec(
		f.ctx,
		`UPDATE jobs SET status = 'Failed' WHERE id = $1`,
		f.jobID,
	)
	if err != nil {
		t.Fatalf("mark job failed: %v", err)
	}

	provider := &mockProvider{
		lookupResult: ProviderResult{
			Outcome:           ProviderSucceeded,
			ProviderReference: "PAY-RECOVERED-001",
			Data:              []byte(`{"status":"SUCCESS"}`),
		},
	}

	processor, err := NewRecoveryProcessor(
		f.pool,
		map[string]Provider{
			"payment-provider": provider,
		},
		"recovery-worker-A",
		time.Minute,
		5,
	)
	if err != nil {
		t.Fatalf("create recovery processor: %v", err)
	}

	processed, err := processor.RunOnce(f.ctx)
	if err != nil {
		t.Fatalf("run recovery processor: %v", err)
	}
	if !processed {
		t.Fatal("expected one operation to be processed")
	}

	executeCalls, lookupCalls := provider.calls()

	if executeCalls != 0 {
		t.Fatalf(
			"expected zero Execute calls during recovery, got %d",
			executeCalls,
		)
	}

	if lookupCalls != 1 {
		t.Fatalf(
			"expected one Lookup call, got %d",
			lookupCalls,
		)
	}

	stored, err := database.GetExecutionOperation(
		f.ctx,
		f.pool,
		operation.ID,
	)
	if err != nil {
		t.Fatalf("get recovered operation: %v", err)
	}

	if stored.Status != "succeeded" {
		t.Fatalf("expected succeeded, got %s", stored.Status)
	}

	if stored.ProviderReference == nil ||
		*stored.ProviderReference != "PAY-RECOVERED-001" {
		t.Fatal("expected recovered provider reference to be persisted")
	}
}

func TestRecoveryProcessorReconcilesFailure(t *testing.T) {
	f := newServiceFixture(t)

	request := f.request(
		"charge-payment",
		"recovery-processor-failure",
		"payment-provider",
		[]byte(`{"amount":1000}`),
	)

	operation, _, err := database.ReserveExecutionOperation(
		f.ctx,
		f.pool,
		f.databaseInput(request),
	)
	if err != nil {
		t.Fatalf("reserve operation: %v", err)
	}

	if err := database.MarkExecutionOutcomeUnknown(
		f.ctx,
		f.pool,
		operation.ID,
		f.fencingToken,
		"provider response timed out",
	); err != nil {
		t.Fatalf("mark outcome unknown: %v", err)
	}

	_, err = f.pool.Exec(
		f.ctx,
		`UPDATE jobs SET status = 'Failed' WHERE id = $1`,
		f.jobID,
	)
	if err != nil {
		t.Fatalf("mark job failed: %v", err)
	}

	provider := &mockProvider{
		lookupResult: ProviderResult{
			Outcome: ProviderFailed,
		},
	}

	processor, err := NewRecoveryProcessor(
		f.pool,
		map[string]Provider{"payment-provider": provider},
		"recovery-worker-B",
		time.Minute,
		5,
	)
	if err != nil {
		t.Fatalf("create recovery processor: %v", err)
	}

	processed, err := processor.RunOnce(f.ctx)
	if err != nil {
		t.Fatalf("run recovery: %v", err)
	}
	if !processed {
		t.Fatal("expected operation to be processed")
	}

	executeCalls, lookupCalls := provider.calls()
	if executeCalls != 0 || lookupCalls != 1 {
		t.Fatalf(
			"expected Execute=0 and Lookup=1, got Execute=%d Lookup=%d",
			executeCalls,
			lookupCalls,
		)
	}

	stored, err := database.GetExecutionOperation(
		f.ctx,
		f.pool,
		operation.ID,
	)
	if err != nil {
		t.Fatalf("get operation: %v", err)
	}

	if stored.Status != "failed" {
		t.Fatalf("expected failed, got %s", stored.Status)
	}
	if stored.NextRecoveryAt != nil {
		t.Fatal("failed operation must not have another recovery scheduled")
	}
	if stored.RecoveryWorkerID != nil ||
		stored.RecoveryLeaseUntil != nil {
		t.Fatal("recovery ownership should be cleared")
	}
}

func TestRecoveryProcessorReschedulesPendingOutcome(t *testing.T) {
	f := newServiceFixture(t)

	request := f.request(
		"charge-payment",
		"recovery-processor-pending",
		"payment-provider",
		[]byte(`{"amount":1000}`),
	)

	operation, _, err := database.ReserveExecutionOperation(
		f.ctx,
		f.pool,
		f.databaseInput(request),
	)
	if err != nil {
		t.Fatalf("reserve operation: %v", err)
	}

	if err := database.MarkExecutionOutcomeUnknown(
		f.ctx,
		f.pool,
		operation.ID,
		f.fencingToken,
		"provider response timed out",
	); err != nil {
		t.Fatalf("mark outcome unknown: %v", err)
	}

	_, err = f.pool.Exec(
		f.ctx,
		`UPDATE jobs SET status = 'Failed' WHERE id = $1`,
		f.jobID,
	)
	if err != nil {
		t.Fatalf("mark job failed: %v", err)
	}

	provider := &mockProvider{
		lookupResult: ProviderResult{
			Outcome: ProviderPending,
		},
	}

	processor, err := NewRecoveryProcessor(
		f.pool,
		map[string]Provider{"payment-provider": provider},
		"recovery-worker-C",
		time.Minute,
		5,
	)
	if err != nil {
		t.Fatalf("create recovery processor: %v", err)
	}

	processed, err := processor.RunOnce(f.ctx)
	if err != nil {
		t.Fatalf("run recovery: %v", err)
	}
	if !processed {
		t.Fatal("expected operation to be processed")
	}

	executeCalls, lookupCalls := provider.calls()
	if executeCalls != 0 || lookupCalls != 1 {
		t.Fatalf(
			"expected Execute=0 and Lookup=1, got Execute=%d Lookup=%d",
			executeCalls,
			lookupCalls,
		)
	}

	stored, err := database.GetExecutionOperation(
		f.ctx,
		f.pool,
		operation.ID,
	)
	if err != nil {
		t.Fatalf("get operation: %v", err)
	}

	if stored.Status != "outcome_unknown" {
		t.Fatalf("expected outcome_unknown, got %s", stored.Status)
	}
	if stored.NextRecoveryAt == nil {
		t.Fatal("expected another recovery attempt to be scheduled")
	}
	if !stored.NextRecoveryAt.After(time.Now()) {
		t.Fatal("expected recovery to be scheduled in the future")
	}
	if stored.RecoveryAttemptCount != 1 {
		t.Fatalf(
			"expected one recovery attempt, got %d",
			stored.RecoveryAttemptCount,
		)
	}
	if stored.RecoveryWorkerID != nil ||
		stored.RecoveryLeaseUntil != nil {
		t.Fatal("recovery ownership should be cleared after rescheduling")
	}
}

func TestRecoveryProcessorReschedulesLookupError(t *testing.T) {
	f := newServiceFixture(t)

	request := f.request(
		"charge-payment",
		"recovery-lookup-error",
		"payment-provider",
		[]byte(`{"amount":1000}`),
	)

	operation, _, err := database.ReserveExecutionOperation(
		f.ctx,
		f.pool,
		f.databaseInput(request),
	)
	if err != nil {
		t.Fatalf("reserve operation: %v", err)
	}

	if err := database.MarkExecutionOutcomeUnknown(
		f.ctx,
		f.pool,
		operation.ID,
		f.fencingToken,
		"provider response timed out",
	); err != nil {
		t.Fatalf("mark outcome unknown: %v", err)
	}

	_, err = f.pool.Exec(
		f.ctx,
		`UPDATE jobs SET status = 'Failed' WHERE id = $1`,
		f.jobID,
	)
	if err != nil {
		t.Fatalf("mark job failed: %v", err)
	}

	provider := &mockProvider{
		lookupErr: errors.New("provider temporarily unavailable"),
	}

	processor, err := NewRecoveryProcessor(
		f.pool,
		map[string]Provider{"payment-provider": provider},
		"recovery-worker-D",
		time.Minute,
		5,
	)
	if err != nil {
		t.Fatalf("create recovery processor: %v", err)
	}

	processed, err := processor.RunOnce(f.ctx)
	if err != nil {
		t.Fatalf("run recovery: %v", err)
	}
	if !processed {
		t.Fatal("expected operation to be processed")
	}

	executeCalls, lookupCalls := provider.calls()
	if executeCalls != 0 || lookupCalls != 1 {
		t.Fatalf(
			"expected Execute=0 and Lookup=1, got Execute=%d Lookup=%d",
			executeCalls,
			lookupCalls,
		)
	}

	stored, err := database.GetExecutionOperation(
		f.ctx,
		f.pool,
		operation.ID,
	)
	if err != nil {
		t.Fatalf("get operation: %v", err)
	}

	if stored.Status != "outcome_unknown" {
		t.Fatalf("expected outcome_unknown, got %s", stored.Status)
	}
	if stored.NextRecoveryAt == nil ||
		!stored.NextRecoveryAt.After(time.Now()) {
		t.Fatal("expected another recovery attempt in the future")
	}
	if stored.RecoveryWorkerID != nil ||
		stored.RecoveryLeaseUntil != nil {
		t.Fatal("recovery ownership should be cleared")
	}
}

func TestRecoveryProcessorReschedulesMissingProvider(t *testing.T) {
	f := newServiceFixture(t)

	request := f.request(
		"charge-payment",
		"recovery-missing-provider",
		"payment-provider",
		[]byte(`{"amount":1000}`),
	)

	operation, _, err := database.ReserveExecutionOperation(
		f.ctx,
		f.pool,
		f.databaseInput(request),
	)
	if err != nil {
		t.Fatalf("reserve operation: %v", err)
	}

	if err := database.MarkExecutionOutcomeUnknown(
		f.ctx,
		f.pool,
		operation.ID,
		f.fencingToken,
		"provider response timed out",
	); err != nil {
		t.Fatalf("mark outcome unknown: %v", err)
	}

	_, err = f.pool.Exec(
		f.ctx,
		`UPDATE jobs SET status = 'Failed' WHERE id = $1`,
		f.jobID,
	)
	if err != nil {
		t.Fatalf("mark job failed: %v", err)
	}

	otherProvider := &mockProvider{}

	processor, err := NewRecoveryProcessor(
		f.pool,
		map[string]Provider{"another-provider": otherProvider},
		"recovery-worker-E",
		time.Minute,
		5,
	)
	if err != nil {
		t.Fatalf("create recovery processor: %v", err)
	}

	processed, err := processor.RunOnce(f.ctx)
	if err != nil {
		t.Fatalf("run recovery: %v", err)
	}
	if !processed {
		t.Fatal("expected operation to be processed")
	}

	executeCalls, lookupCalls := otherProvider.calls()
	if executeCalls != 0 || lookupCalls != 0 {
		t.Fatalf(
			"unexpected calls to unrelated provider: Execute=%d Lookup=%d",
			executeCalls,
			lookupCalls,
		)
	}

	stored, err := database.GetExecutionOperation(
		f.ctx,
		f.pool,
		operation.ID,
	)
	if err != nil {
		t.Fatalf("get operation: %v", err)
	}

	if stored.Status != "outcome_unknown" {
		t.Fatalf("expected outcome_unknown, got %s", stored.Status)
	}
	if stored.NextRecoveryAt == nil ||
		!stored.NextRecoveryAt.After(time.Now()) {
		t.Fatal("expected another recovery attempt in the future")
	}
	if stored.RecoveryWorkerID != nil ||
		stored.RecoveryLeaseUntil != nil {
		t.Fatal("recovery ownership should be cleared")
	}
}

func TestRecoveryProcessorReturnsIdleWhenNoOperationExists(t *testing.T) {
	f := newServiceFixture(t)

	processor, err := NewRecoveryProcessor(
		f.pool,
		map[string]Provider{
			"payment-provider": &mockProvider{},
		},
		"recovery-worker-idle",
		time.Minute,
		5,
	)
	if err != nil {
		t.Fatalf("create recovery processor: %v", err)
	}

	processed, err := processor.RunOnce(f.ctx)
	if err != nil {
		t.Fatalf("run recovery: %v", err)
	}
	if processed {
		t.Fatal("expected no operation to be processed")
	}
}

func TestRecoveryProcessorStopsAfterMaximumAttempts(t *testing.T) {
	f := newServiceFixture(t)

	request := f.request(
		"charge-payment",
		"recovery-max-attempts",
		"payment-provider",
		[]byte(`{"amount":1000}`),
	)

	operation, _, err := database.ReserveExecutionOperation(
		f.ctx,
		f.pool,
		f.databaseInput(request),
	)
	if err != nil {
		t.Fatalf("reserve operation: %v", err)
	}

	if err := database.MarkExecutionOutcomeUnknown(
		f.ctx,
		f.pool,
		operation.ID,
		f.fencingToken,
		"provider response timed out",
	); err != nil {
		t.Fatalf("mark outcome unknown: %v", err)
	}

	_, err = f.pool.Exec(
		f.ctx,
		`UPDATE jobs SET status = 'Failed' WHERE id = $1`,
		f.jobID,
	)
	if err != nil {
		t.Fatalf("mark job failed: %v", err)
	}

	provider := &mockProvider{
		lookupResult: ProviderResult{
			Outcome: ProviderPending,
		},
	}

	processor, err := NewRecoveryProcessor(
		f.pool,
		map[string]Provider{"payment-provider": provider},
		"recovery-worker-limited",
		time.Minute,
		1,
	)
	if err != nil {
		t.Fatalf("create recovery processor: %v", err)
	}

	processed, err := processor.RunOnce(f.ctx)
	if err != nil {
		t.Fatalf("first recovery: %v", err)
	}
	if !processed {
		t.Fatal("expected first recovery attempt to process operation")
	}

	stored, err := database.GetExecutionOperation(
		f.ctx,
		f.pool,
		operation.ID,
	)
	if err != nil {
		t.Fatalf("get operation: %v", err)
	}

	if stored.RecoveryAttemptCount != 1 {
		t.Fatalf("expected one recovery attempt, got %d",
			stored.RecoveryAttemptCount)
	}
	if stored.NextRecoveryAt != nil {
		t.Fatal("expected no further recovery after maximum attempts")
	}
	if stored.Status != "outcome_unknown" {
		t.Fatalf("expected outcome_unknown, got %s", stored.Status)
	}

	processed, err = processor.RunOnce(f.ctx)
	if err != nil {
		t.Fatalf("second recovery: %v", err)
	}
	if processed {
		t.Fatal("operation should not be claimed after exhausting attempts")
	}

	executeCalls, lookupCalls := provider.calls()
	if executeCalls != 0 || lookupCalls != 1 {
		t.Fatalf(
			"expected Execute=0 and Lookup=1, got Execute=%d Lookup=%d",
			executeCalls,
			lookupCalls,
		)
	}
}
