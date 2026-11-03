package execution

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"testing"
	"time"

	"chronos/database"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

// serviceFixture provides a running workflow task and job
// for OperationService tests.
type serviceFixture struct {
	pool         *pgxpool.Pool
	ctx          context.Context
	cancel       context.CancelFunc
	taskID       int64
	jobID        int
	fencingToken int64
}

func newServiceFixture(t *testing.T) *serviceFixture {
	t.Helper()

	if err := godotenv.Load("../.env"); err != nil {
		t.Log(".env not found; using system environment variables")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		15*time.Second,
	)

	pool, err := database.Connect(ctx)
	if err != nil {
		cancel()
		t.Fatalf("connect database: %v", err)
	}

	baseID := time.Now().UnixNano()

	workflowID := baseID
	taskID := baseID + 1
	jobID := int(baseID%700_000_000) + 1_000_000_000
	fencingToken := int64(7)

	tx, err := pool.Begin(ctx)
	if err != nil {
		pool.Close()
		cancel()
		t.Fatalf("begin fixture transaction: %v", err)
	}

	_, err = tx.Exec(
		ctx,
		`INSERT INTO workflows (id, name, status)
		 VALUES ($1, $2, $3)`,
		workflowID,
		"operation-service-test",
		"Running",
	)
	if err != nil {
		_ = tx.Rollback(ctx)
		pool.Close()
		cancel()
		t.Fatalf("insert workflow: %v", err)
	}

	_, err = tx.Exec(
		ctx,
		`INSERT INTO jobs (
			id, name, status, retry_count, max_retries,
			worker_id, lease_until, fencing_token
		)
		VALUES (
			$1, $2, 'Running', 0, 3, $3,
			NOW() + INTERVAL '30 minutes', $4
		)`,
		jobID,
		"operation-service-test",
		"operation-test-worker",
		fencingToken,
	)
	if err != nil {
		_ = tx.Rollback(ctx)
		pool.Close()
		cancel()
		t.Fatalf("insert job: %v", err)
	}

	_, err = tx.Exec(
		ctx,
		`INSERT INTO workflow_tasks (
			id, workflow_id, job_id, name,
			status, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, 'Running', NOW(), NOW())`,
		taskID,
		workflowID,
		jobID,
		"operation-service-test",
	)
	if err != nil {
		_ = tx.Rollback(ctx)
		pool.Close()
		cancel()
		t.Fatalf("insert workflow task: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		pool.Close()
		cancel()
		t.Fatalf("commit fixture: %v", err)
	}

	fixture := &serviceFixture{
		pool:         pool,
		ctx:          ctx,
		cancel:       cancel,
		taskID:       taskID,
		jobID:        jobID,
		fencingToken: fencingToken,
	}

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cleanupCancel()

		_, err := pool.Exec(
			cleanupCtx,
			`DELETE FROM execution_operations
			 WHERE workflow_task_id = $1`,
			taskID,
		)
		if err != nil {
			t.Errorf("cleanup execution operations: %v", err)
		}

		_, err = pool.Exec(
			cleanupCtx,
			`DELETE FROM workflow_tasks WHERE id = $1`,
			taskID,
		)
		if err != nil {
			t.Errorf("cleanup workflow task: %v", err)
		}

		_, err = pool.Exec(
			cleanupCtx,
			`DELETE FROM jobs WHERE id = $1`,
			jobID,
		)
		if err != nil {
			t.Errorf("cleanup job: %v", err)
		}

		_, err = pool.Exec(
			cleanupCtx,
			`DELETE FROM workflows WHERE id = $1`,
			workflowID,
		)
		if err != nil {
			t.Errorf("cleanup workflow: %v", err)
		}

		pool.Close()
		cancel()
	})

	return fixture
}

func (f *serviceFixture) request(
	operationName string,
	key string,
	provider string,
	payload []byte,
) OperationRequest {
	return OperationRequest{
		WorkflowTaskID: f.taskID,
		OperationName:  operationName,
		Provider:       provider,
		IdempotencyKey: key,
		FencingToken:   f.fencingToken,
		Payload:        payload,
	}
}

func (f *serviceFixture) databaseInput(
	request OperationRequest,
) database.ExecutionOperationInput {
	hash := sha256.Sum256(request.Payload)

	return database.ExecutionOperationInput{
		WorkflowTaskID: request.WorkflowTaskID,
		OperationName:  request.OperationName,
		Provider:       request.Provider,
		IdempotencyKey: request.IdempotencyKey,
		RequestHash:    hash[:],
		FencingToken:   request.FencingToken,
	}
}

// mockProvider simulates an external provider.
type mockProvider struct {
	mu           sync.Mutex
	result       ProviderResult
	err          error
	lookupResult ProviderResult
	lookupErr    error
	executeCalls int
	lookupCalls  int
}

func (m *mockProvider) Execute(
	ctx context.Context,
	key string,
	payload []byte,
) (ProviderResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.executeCalls++

	return m.result, m.err
}

func (m *mockProvider) Lookup(
	ctx context.Context,
	key string,
) (ProviderResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.lookupCalls++

	return m.lookupResult, m.lookupErr
}

func (m *mockProvider) calls() (execute, lookup int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.executeCalls, m.lookupCalls
}

// Test a new successful operation and replay of its stored result.
func TestOperationServiceExecutesAndReplaysSuccess(t *testing.T) {
	f := newServiceFixture(t)

	payload := []byte(`{"amount":1000,"currency":"INR"}`)

	provider := &mockProvider{
		result: ProviderResult{
			Outcome:           ProviderSucceeded,
			ProviderReference: "PAY-98765",
			Data:              []byte(`{"status":"SUCCESS"}`),
		},
	}

	service, err := NewOperationService(
		f.pool,
		map[string]Provider{
			"payment-provider": provider,
		},
	)
	if err != nil {
		t.Fatalf("create operation service: %v", err)
	}

	request := f.request(
		"charge-payment",
		"payment-task-100",
		"payment-provider",
		payload,
	)

	// First execution.
	first, err := service.Execute(f.ctx, request)
	if err != nil {
		t.Fatalf("first execution: %v", err)
	}

	if first.Replayed {
		t.Fatal("first execution should not be a replay")
	}

	if first.ProviderReference != "PAY-98765" {
		t.Fatalf(
			"expected provider reference PAY-98765, got %s",
			first.ProviderReference,
		)
	}

	if !bytes.Equal(
		first.Data,
		[]byte(`{"status":"SUCCESS"}`),
	) {
		t.Fatalf("unexpected first result: %s", first.Data)
	}

	// Verify the provider was called exactly once.
	executeCalls, _ := provider.calls()

	if executeCalls != 1 {
		t.Fatalf("expected 1 provider call, got %d", executeCalls)
	}

	// Second execution of the same logical operation.
	second, err := service.Execute(f.ctx, request)
	if err != nil {
		t.Fatalf("second execution: %v", err)
	}

	if !second.Replayed {
		t.Fatal("expected stored result to be replayed")
	}

	if second.ProviderReference != first.ProviderReference {
		t.Fatal("provider reference changed during replay")
	}

	if !bytes.Equal(second.Data, first.Data) {
		t.Fatal("result changed during replay")
	}

	// The provider must still have been called only once.
	executeCalls, _ = provider.calls()

	if executeCalls != 1 {
		t.Fatalf(
			"expected provider to be called once, got %d",
			executeCalls,
		)
	}

	// Verify the database record.
	stored, created, err := database.ReserveExecutionOperation(
		f.ctx,
		f.pool,
		f.databaseInput(request),
	)
	if err != nil {
		t.Fatalf("retrieve stored operation: %v", err)
	}

	if created {
		t.Fatal("unexpectedly created another operation")
	}

	if stored.Status != "succeeded" {
		t.Fatalf("expected succeeded, got %s", stored.Status)
	}
}

// Test that a provider's definitive failure is persisted.
func TestOperationServiceHandlesDefinitiveFailure(t *testing.T) {
	f := newServiceFixture(t)

	payload := []byte(`{"amount":1000,"currency":"INR"}`)

	provider := &mockProvider{
		result: ProviderResult{
			Outcome: ProviderFailed,
			Data:    []byte("insufficient funds"),
		},
	}

	service, err := NewOperationService(
		f.pool,
		map[string]Provider{
			"payment-provider": provider,
		},
	)
	if err != nil {
		t.Fatalf("create operation service: %v", err)
	}

	request := f.request(
		"charge-payment",
		"payment-task-failure",
		"payment-provider",
		payload,
	)

	_, err = service.Execute(f.ctx, request)

	if !errors.Is(err, ErrOperationFailed) {
		t.Fatalf("expected ErrOperationFailed, got %v", err)
	}

	stored, created, err := database.ReserveExecutionOperation(
		f.ctx,
		f.pool,
		f.databaseInput(request),
	)
	if err != nil {
		t.Fatalf("retrieve failed operation: %v", err)
	}

	if created {
		t.Fatal("failed operation was unexpectedly recreated")
	}

	if stored.Status != "failed" {
		t.Fatalf("expected failed, got %s", stored.Status)
	}

	if stored.CompletedAt == nil {
		t.Fatal("expected CompletedAt to be set")
	}

	executeCalls, _ := provider.calls()

	if executeCalls != 1 {
		t.Fatalf("expected 1 provider call, got %d", executeCalls)
	}
}

// Test that a communication timeout produces an unknown outcome.
func TestOperationServiceHandlesProviderTimeout(t *testing.T) {
	f := newServiceFixture(t)

	payload := []byte(`{"amount":1000,"currency":"INR"}`)

	provider := &mockProvider{
		err: errors.New("connection timed out"),
	}

	service, err := NewOperationService(
		f.pool,
		map[string]Provider{
			"payment-provider": provider,
		},
	)
	if err != nil {
		t.Fatalf("create operation service: %v", err)
	}

	request := f.request(
		"charge-payment",
		"payment-task-timeout",
		"payment-provider",
		payload,
	)

	_, err = service.Execute(f.ctx, request)

	if !errors.Is(err, ErrOperationOutcomeUnknown) {
		t.Fatalf(
			"expected ErrOperationOutcomeUnknown, got %v",
			err,
		)
	}

	stored, created, err := database.ReserveExecutionOperation(
		f.ctx,
		f.pool,
		f.databaseInput(request),
	)
	if err != nil {
		t.Fatalf("retrieve unknown operation: %v", err)
	}

	if created {
		t.Fatal("unknown operation was unexpectedly recreated")
	}

	if stored.Status != "outcome_unknown" {
		t.Fatalf(
			"expected outcome_unknown, got %s",
			stored.Status,
		)
	}

	if stored.CompletedAt != nil {
		t.Fatal("unknown operation must not have CompletedAt")
	}

	executeCalls, _ := provider.calls()

	if executeCalls != 1 {
		t.Fatalf("expected 1 provider call, got %d", executeCalls)
	}
}

// Test that an existing processing operation does not call
// the provider a second time.
func TestOperationServiceDoesNotRepeatProcessingOperation(t *testing.T) {
	f := newServiceFixture(t)

	payload := []byte(`{"recipient":"alice@example.com"}`)

	provider := &mockProvider{
		result: ProviderResult{
			Outcome: ProviderSucceeded,
		},
	}

	service, err := NewOperationService(
		f.pool,
		map[string]Provider{
			"email-provider": provider,
		},
	)
	if err != nil {
		t.Fatalf("create operation service: %v", err)
	}

	request := f.request(
		"send-confirmation-email",
		"email-task-100",
		"email-provider",
		payload,
	)

	// Simulate an operation that was reserved by another execution.
	_, created, err := database.ReserveExecutionOperation(
		f.ctx,
		f.pool,
		f.databaseInput(request),
	)
	if err != nil {
		t.Fatalf("reserve operation: %v", err)
	}

	if !created {
		t.Fatal("expected initial reservation")
	}

	// The existing processing operation must not be executed again.
	_, err = service.Execute(f.ctx, request)

	if !errors.Is(err, ErrOperationInProgress) {
		t.Fatalf("expected ErrOperationInProgress, got %v", err)
	}

	executeCalls, _ := provider.calls()

	if executeCalls != 0 {
		t.Fatalf(
			"expected zero provider calls, got %d",
			executeCalls,
		)
	}
}

// Test that a worker with an old fencing token is rejected.
func TestOperationServiceRejectsStaleWorker(t *testing.T) {
	f := newServiceFixture(t)

	// Simulate another worker taking ownership of the job.
	newToken := f.fencingToken + 1

	tag, err := f.pool.Exec(
		f.ctx,
		`UPDATE jobs
		 SET fencing_token = $2,
		     worker_id = 'new-worker'
		 WHERE id = $1`,
		f.jobID,
		newToken,
	)
	if err != nil {
		t.Fatalf("update fencing token: %v", err)
	}

	if tag.RowsAffected() != 1 {
		t.Fatal("expected job ownership to be updated")
	}

	provider := &mockProvider{
		result: ProviderResult{
			Outcome: ProviderSucceeded,
		},
	}

	service, err := NewOperationService(
		f.pool,
		map[string]Provider{
			"payment-provider": provider,
		},
	)
	if err != nil {
		t.Fatalf("create operation service: %v", err)
	}

	// Request still carries the old token.
	request := f.request(
		"charge-payment",
		"stale-worker-payment",
		"payment-provider",
		[]byte(`{"amount":1000}`),
	)

	_, err = service.Execute(f.ctx, request)

	if !errors.Is(err, database.ErrExecutionOperationStaleToken) {
		t.Fatalf(
			"expected stale token error, got %v",
			err,
		)
	}

	executeCalls, _ := provider.calls()

	if executeCalls != 0 {
		t.Fatalf(
			"stale worker must not call provider, got %d calls",
			executeCalls,
		)
	}
}

func TestOperationServiceReconcilesUnknownSuccess(t *testing.T) {
	f := newServiceFixture(t)

	request := f.request(
		"charge-payment",
		"payment-recovery-100",
		"payment-provider",
		[]byte(`{"amount":1000,"currency":"INR"}`),
	)

	// Simulate an operation that was already submitted, but
	// whose outcome could not be established.
	operation, created, err := database.ReserveExecutionOperation(
		f.ctx,
		f.pool,
		f.databaseInput(request),
	)
	if err != nil {
		t.Fatalf("reserve operation: %v", err)
	}

	if !created {
		t.Fatal("expected initial operation reservation")
	}

	if err := database.MarkExecutionOutcomeUnknown(
		f.ctx,
		f.pool,
		operation.ID,
		f.fencingToken,
		"original provider request timed out",
	); err != nil {
		t.Fatalf("mark operation unknown: %v", err)
	}

	providerResult := ProviderResult{
		Outcome:           ProviderSucceeded,
		ProviderReference: "PAY-RECOVERED-100",
		Data:              []byte(`{"status":"SUCCESS","amount":1000}`),
	}

	provider := &mockProvider{
		lookupResult: providerResult,
	}

	service, err := NewOperationService(
		f.pool,
		map[string]Provider{
			"payment-provider": provider,
		},
	)
	if err != nil {
		t.Fatalf("create operation service: %v", err)
	}

	// Execute encounters the existing unknown operation.
	// It must reconcile rather than execute another payment.
	result, err := service.Execute(f.ctx, request)
	if err != nil {
		t.Fatalf("reconcile operation: %v", err)
	}

	if !result.Replayed {
		t.Fatal("expected reconciled result to be marked as replayed")
	}

	if result.ProviderReference != providerResult.ProviderReference {
		t.Fatalf(
			"expected provider reference %s, got %s",
			providerResult.ProviderReference,
			result.ProviderReference,
		)
	}

	if !bytes.Equal(result.Data, providerResult.Data) {
		t.Fatalf("unexpected reconciled result: %s", result.Data)
	}

	executeCalls, lookupCalls := provider.calls()

	if executeCalls != 0 {
		t.Fatalf(
			"expected no new provider execution, got %d",
			executeCalls,
		)
	}

	if lookupCalls != 1 {
		t.Fatalf(
			"expected one provider lookup, got %d",
			lookupCalls,
		)
	}

	saved, err := database.GetExecutionOperation(
		f.ctx,
		f.pool,
		operation.ID,
	)
	if err != nil {
		t.Fatalf("get reconciled operation: %v", err)
	}

	if saved.Status != "succeeded" {
		t.Fatalf("expected succeeded, got %s", saved.Status)
	}

	if saved.ProviderReference == nil ||
		*saved.ProviderReference != providerResult.ProviderReference {
		t.Fatal("reconciled provider reference was not saved")
	}

	if !bytes.Equal(saved.Result, providerResult.Data) {
		t.Fatal("reconciled result was not saved")
	}
}

func TestOperationServiceKeepsNotFoundOutcomeUnknown(t *testing.T) {
	f := newServiceFixture(t)

	request := f.request(
		"charge-payment",
		"payment-not-found-100",
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
		t.Fatal("expected initial reservation")
	}

	if err := database.MarkExecutionOutcomeUnknown(
		f.ctx,
		f.pool,
		operation.ID,
		f.fencingToken,
		"original provider request timed out",
	); err != nil {
		t.Fatalf("mark operation unknown: %v", err)
	}

	provider := &mockProvider{
		lookupResult: ProviderResult{
			Outcome: ProviderNotFound,
		},
	}

	service, err := NewOperationService(
		f.pool,
		map[string]Provider{
			"payment-provider": provider,
		},
	)
	if err != nil {
		t.Fatalf("create operation service: %v", err)
	}

	_, err = service.Execute(f.ctx, request)

	if !errors.Is(err, ErrOperationOutcomeUnknown) {
		t.Fatalf(
			"expected unknown outcome error, got %v",
			err,
		)
	}

	executeCalls, lookupCalls := provider.calls()

	if executeCalls != 0 {
		t.Fatalf(
			"provider must not be executed again, got %d calls",
			executeCalls,
		)
	}

	if lookupCalls != 1 {
		t.Fatalf(
			"expected one provider lookup, got %d",
			lookupCalls,
		)
	}

	saved, err := database.GetExecutionOperation(
		f.ctx,
		f.pool,
		operation.ID,
	)
	if err != nil {
		t.Fatalf("get operation: %v", err)
	}

	if saved.Status != "outcome_unknown" {
		t.Fatalf(
			"expected outcome_unknown, got %s",
			saved.Status,
		)
	}
}

func TestMarkUnknownPersistsAfterContextCancellation(t *testing.T) {
	f := newServiceFixture(t)

	request := f.request(
		"charge-payment",
		"payment-cancel-test",
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
		t.Fatal("expected initial operation reservation")
	}

	// Simulate worker cancellation.
	cancelledCtx, cancel := context.WithCancel(f.ctx)
	cancel()

	service := &OperationService{
		pool: f.pool,
	}

	err = service.markUnknown(
		cancelledCtx,
		operation.ID,
		f.fencingToken,
		"provider request timed out during cancellation",
	)

	if !errors.Is(err, ErrOperationOutcomeUnknown) {
		t.Fatalf(
			"expected unknown outcome error, got %v",
			err,
		)
	}

	// Read using the fixture's normal, non-cancelled context.
	saved, err := database.GetExecutionOperation(
		f.ctx,
		f.pool,
		operation.ID,
	)
	if err != nil {
		t.Fatalf("retrieve operation: %v", err)
	}

	if saved.Status != "outcome_unknown" {
		t.Fatalf(
			"expected outcome_unknown, got %s",
			saved.Status,
		)
	}
}
