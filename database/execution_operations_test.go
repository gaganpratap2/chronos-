package database

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

type executionOperationFixture struct {
	pool         *pgxpool.Pool
	ctx          context.Context
	taskID       int64
	jobID        int
	fencingToken int64
}

func newExecutionOperationFixture(
	t *testing.T,
) *executionOperationFixture {
	t.Helper()

	if err := godotenv.Load("../.env"); err != nil {
		t.Log(".env not found; using system environment variables")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		15*time.Second,
	)

	pool, err := Connect(ctx)
	if err != nil {
		cancel()
		t.Fatalf("connect database: %v", err)
	}

	// Generate distinct test identifiers.
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

	// Create the parent workflow.
	_, err = tx.Exec(
		ctx,
		`INSERT INTO workflows (id, name, status)
		 VALUES ($1, $2, $3)`,
		workflowID,
		"execution-idempotency-test",
		"Running",
	)
	if err != nil {
		_ = tx.Rollback(ctx)
		pool.Close()
		cancel()
		t.Fatalf("insert fixture workflow: %v", err)
	}

	// Create a running job with a known fencing token.
	_, err = tx.Exec(
		ctx,
		`INSERT INTO jobs (
			id,
			name,
			status,
			retry_count,
			max_retries,
			worker_id,
			lease_until,
			fencing_token
		)
		VALUES ($1, $2, 'Running', 0, 3, $3,
		        NOW() + INTERVAL '30 minutes', $4)`,
		jobID,
		"execution-idempotency-test",
		"execution-test-worker",
		fencingToken,
	)
	if err != nil {
		_ = tx.Rollback(ctx)
		pool.Close()
		cancel()
		t.Fatalf("insert fixture job: %v", err)
	}

	// Link the workflow task to its execution job.
	_, err = tx.Exec(
		ctx,
		`INSERT INTO workflow_tasks (
			id,
			workflow_id,
			job_id,
			name,
			status,
			created_at,
			updated_at
		)
		VALUES ($1, $2, $3, $4, 'Running', NOW(), NOW())`,
		taskID,
		workflowID,
		jobID,
		"execution-idempotency-test",
	)
	if err != nil {
		_ = tx.Rollback(ctx)
		pool.Close()
		cancel()
		t.Fatalf("insert fixture task: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		pool.Close()
		cancel()
		t.Fatalf("commit fixture: %v", err)
	}

	fixture := &executionOperationFixture{
		pool:         pool,
		ctx:          ctx,
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

		// Delete execution operations before their referenced task.
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
			"DELETE FROM workflow_tasks WHERE id = $1",
			taskID,
		)
		if err != nil {
			t.Errorf("cleanup workflow task: %v", err)
		}

		_, err = pool.Exec(
			cleanupCtx,
			"DELETE FROM jobs WHERE id = $1",
			jobID,
		)
		if err != nil {
			t.Errorf("cleanup job: %v", err)
		}

		_, err = pool.Exec(
			cleanupCtx,
			"DELETE FROM workflows WHERE id = $1",
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

func (f *executionOperationFixture) input(
	operationName string,
	idempotencyKey string,
	payload []byte,
) ExecutionOperationInput {

	hash := sha256.Sum256(payload)

	return ExecutionOperationInput{
		WorkflowTaskID: f.taskID,
		OperationName:  operationName,
		Provider:       "email-service",
		IdempotencyKey: idempotencyKey,
		RequestHash:    hash[:],
		FencingToken:   f.fencingToken,
	}
}

// Test that a new execution operation is reserved successfully.
func TestReserveExecutionOperation(t *testing.T) {
	f := newExecutionOperationFixture(t)

	input := f.input(
		"send-email-alice",
		"task-email-alice",
		[]byte(`{"recipient":"alice@example.com"}`),
	)

	operation, created, err := ReserveExecutionOperation(
		f.ctx,
		f.pool,
		input,
	)
	if err != nil {
		t.Fatalf("reserve operation: %v", err)
	}

	if !created {
		t.Fatal("expected a new operation to be created")
	}

	if operation.ID <= 0 {
		t.Fatalf("expected positive operation ID, got %d", operation.ID)
	}

	if operation.WorkflowTaskID != f.taskID {
		t.Errorf(
			"expected task ID %d, got %d",
			f.taskID,
			operation.WorkflowTaskID,
		)
	}

	if operation.Status != "processing" {
		t.Errorf(
			"expected processing, got %s",
			operation.Status,
		)
	}

	if operation.FencingToken != f.fencingToken {
		t.Errorf(
			"expected fencing token %d, got %d",
			f.fencingToken,
			operation.FencingToken,
		)
	}

	if operation.AttemptCount != 1 {
		t.Errorf(
			"expected attempt count 1, got %d",
			operation.AttemptCount,
		)
	}
}

// Test that reserving the same logical operation does not
// create another execution record.
func TestReserveExecutionOperationReplay(t *testing.T) {
	f := newExecutionOperationFixture(t)

	input := f.input(
		"send-email-alice",
		"task-email-alice",
		[]byte(`{"recipient":"alice@example.com"}`),
	)

	first, created, err := ReserveExecutionOperation(
		f.ctx,
		f.pool,
		input,
	)
	if err != nil {
		t.Fatalf("first reservation: %v", err)
	}

	if !created {
		t.Fatal("expected first reservation to create a record")
	}

	second, created, err := ReserveExecutionOperation(
		f.ctx,
		f.pool,
		input,
	)
	if err != nil {
		t.Fatalf("duplicate reservation: %v", err)
	}

	if created {
		t.Fatal("duplicate reservation created another record")
	}

	if first.ID != second.ID {
		t.Fatalf(
			"expected same operation ID %d, got %d",
			first.ID,
			second.ID,
		)
	}

	if second.Status != "processing" {
		t.Errorf("expected processing, got %s", second.Status)
	}

	if second.AttemptCount != 1 {
		t.Errorf(
			"expected attempt count to remain 1, got %d",
			second.AttemptCount,
		)
	}
}

// Test that the same logical operation cannot silently change
// its original request parameters.
func TestReserveExecutionOperationRejectsChangedPayload(t *testing.T) {
	f := newExecutionOperationFixture(t)

	firstInput := f.input(
		"send-email-alice",
		"task-email-alice",
		[]byte(`{"recipient":"alice@example.com"}`),
	)

	_, _, err := ReserveExecutionOperation(
		f.ctx,
		f.pool,
		firstInput,
	)
	if err != nil {
		t.Fatalf("first reservation: %v", err)
	}

	changedInput := f.input(
		"send-email-alice",
		"task-email-alice",
		[]byte(`{"recipient":"bob@example.com"}`),
	)

	_, _, err = ReserveExecutionOperation(
		f.ctx,
		f.pool,
		changedInput,
	)

	if !errors.Is(err, ErrExecutionOperationConflict) {
		t.Fatalf(
			"expected execution operation conflict, got %v",
			err,
		)
	}
}

// Test that a stale worker cannot reserve a new execution operation.
func TestReserveExecutionOperationRejectsStaleToken(t *testing.T) {
	f := newExecutionOperationFixture(t)

	input := f.input(
		"send-email-alice",
		"task-email-alice",
		[]byte(`{"recipient":"alice@example.com"}`),
	)

	input.FencingToken = f.fencingToken - 1

	_, _, err := ReserveExecutionOperation(
		f.ctx,
		f.pool,
		input,
	)

	if !errors.Is(err, ErrExecutionOperationStaleToken) {
		t.Fatalf(
			"expected stale fencing token error, got %v",
			err,
		)
	}
}

// Test unknown-outcome recovery, ownership takeover,
// stale-worker rejection and successful reconciliation.
func TestExecutionOperationTakeoverAndRecovery(t *testing.T) {
	f := newExecutionOperationFixture(t)

	input := f.input(
		"charge-payment",
		"task-500-payment",
		[]byte(`{"amount":1000,"currency":"INR"}`),
	)

	operation, created, err := ReserveExecutionOperation(
		f.ctx,
		f.pool,
		input,
	)
	if err != nil {
		t.Fatalf("reserve payment operation: %v", err)
	}

	if !created {
		t.Fatal("expected payment operation to be created")
	}

	// Worker A cannot determine whether payment succeeded.
	if err := MarkExecutionOutcomeUnknown(
		f.ctx,
		f.pool,
		operation.ID,
		f.fencingToken,
		"payment provider timed out",
	); err != nil {
		t.Fatalf("mark unknown outcome: %v", err)
	}

	// Verify the uncertain state was persisted.
	unknownOperation, err := GetExecutionOperation(
		f.ctx,
		f.pool,
		operation.ID,
	)
	if err != nil {
		t.Fatalf("retrieve unknown operation: %v", err)
	}

	if unknownOperation.Status != "outcome_unknown" {
		t.Fatalf(
			"expected outcome_unknown, got %s",
			unknownOperation.Status,
		)
	}

	// Simulate Worker B taking over the linked job.
	const newFencingToken int64 = 8

	tag, err := f.pool.Exec(
		f.ctx,
		`UPDATE jobs
		 SET fencing_token = $2,
		     worker_id = 'worker-B',
		     lease_until = NOW() + INTERVAL '30 minutes'
		 WHERE id = $1
		   AND fencing_token = $3
		   AND status = 'Running'`,
		f.jobID,
		newFencingToken,
		f.fencingToken,
	)
	if err != nil {
		t.Fatalf("update job ownership: %v", err)
	}

	if tag.RowsAffected() != 1 {
		t.Fatal("expected job ownership to transfer to Worker B")
	}

	// Worker B takes over the execution record.
	recoveredOperation, err := TakeOverExecutionOperation(
		f.ctx,
		f.pool,
		operation.ID,
		newFencingToken,
	)
	if err != nil {
		t.Fatalf("take over execution operation: %v", err)
	}

	if recoveredOperation.Status != "outcome_unknown" {
		t.Fatalf(
			"expected outcome_unknown after takeover, got %s",
			recoveredOperation.Status,
		)
	}

	if recoveredOperation.FencingToken != newFencingToken {
		t.Fatalf(
			"expected fencing token %d, got %d",
			newFencingToken,
			recoveredOperation.FencingToken,
		)
	}

	if recoveredOperation.AttemptCount != 2 {
		t.Fatalf(
			"expected attempt count 2, got %d",
			recoveredOperation.AttemptCount,
		)
	}

	// The logical operation identity must remain unchanged.
	if recoveredOperation.IdempotencyKey != input.IdempotencyKey {
		t.Fatal("idempotency key changed during takeover")
	}

	if !bytes.Equal(
		recoveredOperation.RequestHash,
		input.RequestHash,
	) {
		t.Fatal("request hash changed during takeover")
	}

	// Worker A must not complete using its old token.
	providerReference := "PAY-98765"
	paymentResult := []byte(`{"status":"SUCCESS","amount":1000}`)

	err = CompleteExecutionOperation(
		f.ctx,
		f.pool,
		operation.ID,
		f.fencingToken,
		&providerReference,
		paymentResult,
	)

	if !errors.Is(err, ErrExecutionOperationStaleToken) {
		t.Fatalf(
			"expected stale worker rejection, got %v",
			err,
		)
	}

	// Worker B has reconciled the original provider operation
	// and now records its confirmed success.
	err = CompleteExecutionOperation(
		f.ctx,
		f.pool,
		operation.ID,
		newFencingToken,
		&providerReference,
		paymentResult,
	)
	if err != nil {
		t.Fatalf("complete reconciled operation: %v", err)
	}

	// Verify the final database result.
	saved, err := GetExecutionOperation(
		f.ctx,
		f.pool,
		operation.ID,
	)
	if err != nil {
		t.Fatalf("retrieve completed operation: %v", err)
	}

	if saved.Status != "succeeded" {
		t.Fatalf(
			"expected succeeded, got %s",
			saved.Status,
		)
	}

	if saved.ProviderReference == nil ||
		*saved.ProviderReference != providerReference {
		t.Fatal("provider reference was not persisted correctly")
	}

	if !bytes.Equal(saved.Result, paymentResult) {
		t.Fatalf(
			"expected result %s, got %s",
			paymentResult,
			saved.Result,
		)
	}

	if saved.CompletedAt == nil {
		t.Fatal("expected CompletedAt to be set")
	}
}

// Test definitive failure and prevent reopening a terminal operation.
func TestExecutionOperationDefinitiveFailure(t *testing.T) {
	f := newExecutionOperationFixture(t)

	input := f.input(
		"charge-payment",
		"task-501-payment",
		[]byte(`{"amount":1000,"currency":"INR"}`),
	)

	operation, _, err := ReserveExecutionOperation(
		f.ctx,
		f.pool,
		input,
	)
	if err != nil {
		t.Fatalf("reserve payment operation: %v", err)
	}

	const failureMessage = "provider confirmed insufficient funds"

	err = FailExecutionOperation(
		f.ctx,
		f.pool,
		operation.ID,
		f.fencingToken,
		failureMessage,
	)
	if err != nil {
		t.Fatalf("fail execution operation: %v", err)
	}

	saved, err := GetExecutionOperation(
		f.ctx,
		f.pool,
		operation.ID,
	)
	if err != nil {
		t.Fatalf("retrieve failed operation: %v", err)
	}

	if saved.Status != "failed" {
		t.Fatalf("expected failed, got %s", saved.Status)
	}

	if saved.LastError == nil ||
		*saved.LastError != failureMessage {
		t.Fatal("expected failure reason to be persisted")
	}

	if saved.CompletedAt == nil {
		t.Fatal("expected CompletedAt to be set for terminal failure")
	}

	// A terminally failed operation cannot become successful
	// without a new logical business operation.
	err = CompleteExecutionOperation(
		f.ctx,
		f.pool,
		operation.ID,
		f.fencingToken,
		nil,
		[]byte(`{"status":"SUCCESS"}`),
	)

	if !errors.Is(err, ErrExecutionOperationInvalidState) {
		t.Fatalf(
			"expected invalid state error, got %v",
			err,
		)
	}
}

// Test that one provider idempotency key cannot be reused
// by a different workflow task.
func TestReserveExecutionOperationRejectsProviderKeyReuse(t *testing.T) {
	f := newExecutionOperationFixture(t)

	firstInput := f.input(
		"send-email-alice",
		"shared-provider-key",
		[]byte(`{"recipient":"alice@example.com"}`),
	)

	_, _, err := ReserveExecutionOperation(
		f.ctx,
		f.pool,
		firstInput,
	)
	if err != nil {
		t.Fatalf("reserve first operation: %v", err)
	}

	// Create another workflow task linked to the same running job.
	secondTaskID := f.taskID + 1
	workflowID := f.taskID - 1

	_, err = f.pool.Exec(
		f.ctx,
		`INSERT INTO workflow_tasks (
			id,
			workflow_id,
			job_id,
			name,
			status,
			created_at,
			updated_at
		)
		VALUES ($1, $2, $3, $4, 'Running', NOW(), NOW())`,
		secondTaskID,
		workflowID,
		f.jobID,
		"second-execution-test-task",
	)
	if err != nil {
		t.Fatalf("create second workflow task: %v", err)
	}

	// Clean up the second task before the fixture removes
	// its parent workflow and linked job.
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()

		_, err := f.pool.Exec(
			cleanupCtx,
			"DELETE FROM workflow_tasks WHERE id = $1",
			secondTaskID,
		)
		if err != nil {
			t.Errorf("cleanup second workflow task: %v", err)
		}
	})

	secondInput := f.input(
		"send-email-bob",
		"shared-provider-key",
		[]byte(`{"recipient":"bob@example.com"}`),
	)

	secondInput.WorkflowTaskID = secondTaskID

	_, _, err = ReserveExecutionOperation(
		f.ctx,
		f.pool,
		secondInput,
	)

	if !errors.Is(err, ErrExecutionOperationConflict) {
		t.Fatalf(
			"expected provider key conflict, got %v",
			err,
		)
	}
}

func TestReserveExecutionOperationConcurrently(t *testing.T) {
	f := newExecutionOperationFixture(t)

	input := f.input(
		"send-email-alice",
		"task-email-alice",
		[]byte(`{"recipient":"alice@example.com"}`),
	)

	type reservationResult struct {
		operation ExecutionOperation
		created   bool
		err       error
	}

	const callers = 2

	start := make(chan struct{})
	results := make(chan reservationResult, callers)

	var wg sync.WaitGroup

	for i := 0; i < callers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			<-start

			operation, created, err :=
				ReserveExecutionOperation(
					f.ctx,
					f.pool,
					input,
				)

			results <- reservationResult{
				operation: operation,
				created:   created,
				err:       err,
			}
		}()
	}

	close(start)

	wg.Wait()
	close(results)

	var createdCount int
	var existingCount int
	var operationID int64

	for result := range results {
		if result.err != nil {
			t.Fatalf("concurrent reservation failed: %v", result.err)
		}

		if result.operation.ID <= 0 {
			t.Fatal("expected valid operation ID")
		}

		if operationID == 0 {
			operationID = result.operation.ID
		} else if operationID != result.operation.ID {
			t.Fatalf(
				"expected same operation ID %d, got %d",
				operationID,
				result.operation.ID,
			)
		}

		if result.created {
			createdCount++
		} else {
			existingCount++
		}
	}

	if createdCount != 1 {
		t.Fatalf(
			"expected exactly one creation, got %d",
			createdCount,
		)
	}

	if existingCount != 1 {
		t.Fatalf(
			"expected exactly one existing reservation, got %d",
			existingCount,
		)
	}

	var count int

	err := f.pool.QueryRow(
		f.ctx,
		`SELECT COUNT(*)
		 FROM execution_operations
		 WHERE workflow_task_id = $1
		   AND operation_name = $2`,
		f.taskID,
		input.OperationName,
	).Scan(&count)

	if err != nil {
		t.Fatalf("count execution operations: %v", err)
	}

	if count != 1 {
		t.Fatalf(
			"expected exactly one operation record, got %d",
			count,
		)
	}
}

func TestReserveExecutionOperationReturnsCompletedResult(t *testing.T) {
	f := newExecutionOperationFixture(t)

	input := f.input(
		"charge-payment",
		"task-payment-500",
		[]byte(`{"amount":1000,"currency":"INR"}`),
	)

	original, created, err := ReserveExecutionOperation(
		f.ctx,
		f.pool,
		input,
	)
	if err != nil {
		t.Fatalf("reserve operation: %v", err)
	}

	if !created {
		t.Fatal("expected initial reservation")
	}

	providerReference := "PAY-98765"
	originalResult := []byte(
		`{"status":"SUCCESS","amount":1000}`,
	)

	err = CompleteExecutionOperation(
		f.ctx,
		f.pool,
		original.ID,
		f.fencingToken,
		&providerReference,
		originalResult,
	)
	if err != nil {
		t.Fatalf("complete operation: %v", err)
	}

	// Simulate another worker attempting the same operation.
	replayed, created, err := ReserveExecutionOperation(
		f.ctx,
		f.pool,
		input,
	)
	if err != nil {
		t.Fatalf("replay completed operation: %v", err)
	}

	if created {
		t.Fatal("completed operation was created again")
	}

	if replayed.ID != original.ID {
		t.Fatalf("expected original operation ID %d, got %d",
			original.ID, replayed.ID)
	}

	if replayed.Status != "succeeded" {
		t.Fatalf("expected succeeded, got %s", replayed.Status)
	}

	if !bytes.Equal(replayed.Result, originalResult) {
		t.Fatalf(
			"expected original result %s, got %s",
			originalResult,
			replayed.Result,
		)
	}

	if replayed.ProviderReference == nil ||
		*replayed.ProviderReference != providerReference {
		t.Fatal("original provider reference was not preserved")
	}
}

func TestCompleteExecutionOperationRecovery(t *testing.T) {
	f := newExecutionOperationFixture(t)

	input := f.input(
		"charge-payment",
		"recovery-success-test",
		[]byte(`{"amount":1000,"currency":"INR"}`),
	)

	operation, created, err := ReserveExecutionOperation(
		f.ctx,
		f.pool,
		input,
	)
	if err != nil {
		t.Fatalf("reserve operation: %v", err)
	}
	if !created {
		t.Fatal("expected operation to be created")
	}

	if err := MarkExecutionOutcomeUnknown(
		f.ctx,
		f.pool,
		operation.ID,
		f.fencingToken,
		"provider response timed out",
	); err != nil {
		t.Fatalf("mark operation unknown: %v", err)
	}

	// Recovery processor only handles operations linked to Failed jobs.
	_, err = f.pool.Exec(
		f.ctx,
		`UPDATE jobs SET status = 'Failed' WHERE id = $1`,
		f.jobID,
	)
	if err != nil {
		t.Fatalf("mark job failed: %v", err)
	}

	recovered, claimed, err := ClaimNextExecutionOperationRecovery(
		f.ctx,
		f.pool,
		"recovery-worker-A",
		time.Minute,
		5,
	)
	if err != nil {
		t.Fatalf("claim recovery operation: %v", err)
	}
	if !claimed {
		t.Fatal("expected recovery operation to be claimed")
	}

	if recovered.ID != operation.ID {
		t.Fatalf(
			"expected operation ID %d, got %d",
			operation.ID,
			recovered.ID,
		)
	}

	if recovered.RecoveryAttemptCount != 1 {
		t.Fatalf(
			"expected recovery attempt count 1, got %d",
			recovered.RecoveryAttemptCount,
		)
	}

	providerReference := "PAY-RECOVERED-001"
	result := []byte(`{"status":"SUCCESS","amount":1000}`)

	completed, err := CompleteExecutionOperationRecovery(
		f.ctx,
		f.pool,
		recovered.ID,
		"recovery-worker-A",
		recovered.RecoveryFencingToken,
		&providerReference,
		result,
	)
	if err != nil {
		t.Fatalf("complete recovery: %v", err)
	}

	if completed.Status != "succeeded" {
		t.Fatalf("expected succeeded, got %s", completed.Status)
	}

	if completed.RecoveryWorkerID != nil {
		t.Fatal("expected recovery worker ownership to be cleared")
	}

	if completed.RecoveryLeaseUntil != nil {
		t.Fatal("expected recovery lease to be cleared")
	}

	if completed.NextRecoveryAt != nil {
		t.Fatal("expected next recovery time to be cleared")
	}

	// Verify the final state persisted in PostgreSQL.
	stored, err := GetExecutionOperation(
		f.ctx,
		f.pool,
		operation.ID,
	)
	if err != nil {
		t.Fatalf("get completed operation: %v", err)
	}

	if stored.Status != "succeeded" {
		t.Fatalf("expected persisted succeeded status, got %s", stored.Status)
	}
}

func TestFailExecutionOperationRecovery(t *testing.T) {
	f := newExecutionOperationFixture(t)

	input := f.input(
		"charge-payment",
		"recovery-failure-test",
		[]byte(`{"amount":1000,"currency":"INR"}`),
	)

	operation, created, err := ReserveExecutionOperation(
		f.ctx, f.pool, input,
	)
	if err != nil {
		t.Fatalf("reserve operation: %v", err)
	}
	if !created {
		t.Fatal("expected operation to be created")
	}

	if err := MarkExecutionOutcomeUnknown(
		f.ctx, f.pool, operation.ID,
		f.fencingToken, "provider timed out",
	); err != nil {
		t.Fatalf("mark unknown: %v", err)
	}

	_, err = f.pool.Exec(
		f.ctx,
		`UPDATE jobs SET status = 'Failed' WHERE id = $1`,
		f.jobID,
	)
	if err != nil {
		t.Fatalf("mark job failed: %v", err)
	}

	claimedOperation, claimed, err := ClaimNextExecutionOperationRecovery(
		f.ctx, f.pool, "recovery-worker-A", time.Minute, 5,
	)
	if err != nil {
		t.Fatalf("claim recovery: %v", err)
	}
	if !claimed {
		t.Fatal("expected recovery claim")
	}

	failed, err := FailExecutionOperationRecovery(
		f.ctx,
		f.pool,
		claimedOperation.ID,
		"recovery-worker-A",
		claimedOperation.RecoveryFencingToken,
		"provider confirmed payment was declined",
	)
	if err != nil {
		t.Fatalf("fail recovery: %v", err)
	}

	if failed.Status != "failed" {
		t.Fatalf("expected failed, got %s", failed.Status)
	}

	if failed.NextRecoveryAt != nil {
		t.Fatal("expected recovery schedule to be cleared")
	}
	if failed.RecoveryWorkerID != nil {
		t.Fatal("expected recovery ownership to be cleared")
	}
	if failed.RecoveryLeaseUntil != nil {
		t.Fatal("expected recovery lease to be cleared")
	}

	stored, err := GetExecutionOperation(f.ctx, f.pool, operation.ID)
	if err != nil {
		t.Fatalf("get operation: %v", err)
	}
	if stored.Status != "failed" {
		t.Fatalf("expected persisted failed status, got %s", stored.Status)
	}
}

func TestRescheduleExecutionOperationRecovery(t *testing.T) {
	f := newExecutionOperationFixture(t)

	input := f.input(
		"charge-payment",
		"recovery-reschedule-test",
		[]byte(`{"amount":1000,"currency":"INR"}`),
	)

	operation, created, err := ReserveExecutionOperation(
		f.ctx, f.pool, input,
	)
	if err != nil {
		t.Fatalf("reserve operation: %v", err)
	}
	if !created {
		t.Fatal("expected operation to be created")
	}

	if err := MarkExecutionOutcomeUnknown(
		f.ctx, f.pool, operation.ID,
		f.fencingToken, "provider timed out",
	); err != nil {
		t.Fatalf("mark unknown: %v", err)
	}

	_, err = f.pool.Exec(
		f.ctx,
		`UPDATE jobs SET status = 'Failed' WHERE id = $1`,
		f.jobID,
	)
	if err != nil {
		t.Fatalf("mark job failed: %v", err)
	}

	// First recovery attempt.
	claimedOperation, claimed, err := ClaimNextExecutionOperationRecovery(
		f.ctx, f.pool, "recovery-worker-A", time.Minute, 2,
	)
	if err != nil {
		t.Fatalf("claim first recovery: %v", err)
	}
	if !claimed {
		t.Fatal("expected first recovery claim")
	}

	firstDelay := 30 * time.Second

	rescheduled, err := RescheduleExecutionOperationRecovery(
		f.ctx,
		f.pool,
		claimedOperation.ID,
		"recovery-worker-A",
		claimedOperation.RecoveryFencingToken,
		"provider still cannot confirm outcome",
		firstDelay,
		2,
	)
	if err != nil {
		t.Fatalf("reschedule first recovery: %v", err)
	}

	if rescheduled.Status != "outcome_unknown" {
		t.Fatalf("expected outcome_unknown, got %s", rescheduled.Status)
	}
	if rescheduled.NextRecoveryAt == nil {
		t.Fatal("expected another recovery attempt to be scheduled")
	}
	if rescheduled.RecoveryWorkerID != nil {
		t.Fatal("expected recovery ownership to be cleared")
	}

	// Make the next attempt due immediately for deterministic testing.
	_, err = f.pool.Exec(
		f.ctx,
		`UPDATE execution_operations
         SET next_recovery_at = NOW()
         WHERE id = $1`,
		operation.ID,
	)
	if err != nil {
		t.Fatalf("make recovery due: %v", err)
	}

	// Second and final recovery attempt.
	finalClaim, claimed, err := ClaimNextExecutionOperationRecovery(
		f.ctx, f.pool, "recovery-worker-B", time.Minute, 2,
	)
	if err != nil {
		t.Fatalf("claim final recovery: %v", err)
	}
	if !claimed {
		t.Fatal("expected final recovery claim")
	}

	if finalClaim.RecoveryAttemptCount != 2 {
		t.Fatalf(
			"expected 2 recovery attempts, got %d",
			finalClaim.RecoveryAttemptCount,
		)
	}

	exhausted, err := RescheduleExecutionOperationRecovery(
		f.ctx,
		f.pool,
		finalClaim.ID,
		"recovery-worker-B",
		finalClaim.RecoveryFencingToken,
		"provider still cannot confirm outcome",
		60*time.Second,
		2,
	)
	if err != nil {
		t.Fatalf("reschedule final recovery: %v", err)
	}

	if exhausted.Status != "outcome_unknown" {
		t.Fatalf("expected outcome_unknown, got %s", exhausted.Status)
	}
	if exhausted.NextRecoveryAt != nil {
		t.Fatal("expected automatic recovery to stop at attempt limit")
	}
	if exhausted.RecoveryWorkerID != nil {
		t.Fatal("expected recovery worker ownership to be cleared")
	}

	// The exhausted operation must not be claimable again.
	_, claimed, err = ClaimNextExecutionOperationRecovery(
		f.ctx, f.pool, "recovery-worker-C", time.Minute, 2,
	)
	if err != nil {
		t.Fatalf("check exhausted operation: %v", err)
	}
	if claimed {
		t.Fatal("exhausted operation must not be claimed again")
	}
}

func TestConcurrentExecutionRecoveryClaims(t *testing.T) {
	f := newExecutionOperationFixture(t)

	input := f.input(
		"charge-payment",
		"recovery-concurrent-claim-test",
		[]byte(`{"amount":1000,"currency":"INR"}`),
	)

	operation, created, err := ReserveExecutionOperation(
		f.ctx, f.pool, input,
	)
	if err != nil {
		t.Fatalf("reserve operation: %v", err)
	}
	if !created {
		t.Fatal("expected operation to be created")
	}

	if err := MarkExecutionOutcomeUnknown(
		f.ctx, f.pool, operation.ID,
		f.fencingToken, "provider timed out",
	); err != nil {
		t.Fatalf("mark unknown: %v", err)
	}

	_, err = f.pool.Exec(
		f.ctx,
		`UPDATE jobs SET status = 'Failed' WHERE id = $1`,
		f.jobID,
	)
	if err != nil {
		t.Fatalf("mark job failed: %v", err)
	}

	type claimResult struct {
		workerID string
		claimed  bool
		err      error
	}

	start := make(chan struct{})
	results := make(chan claimResult, 2)

	var wg sync.WaitGroup

	workers := []string{
		"recovery-worker-A",
		"recovery-worker-B",
	}

	for _, workerID := range workers {
		wg.Add(1)

		go func(id string) {
			defer wg.Done()

			<-start

			_, claimed, err := ClaimNextExecutionOperationRecovery(
				f.ctx,
				f.pool,
				id,
				time.Minute,
				5,
			)

			results <- claimResult{
				workerID: id,
				claimed:  claimed,
				err:      err,
			}
		}(workerID)
	}

	close(start)
	wg.Wait()
	close(results)

	successfulClaims := 0

	for result := range results {
		if result.err != nil {
			t.Fatalf(
				"%s claim failed: %v",
				result.workerID,
				result.err,
			)
		}

		if result.claimed {
			successfulClaims++
		}
	}

	if successfulClaims != 1 {
		t.Fatalf(
			"expected exactly one successful claim, got %d",
			successfulClaims,
		)
	}

	stored, err := GetExecutionOperation(
		f.ctx, f.pool, operation.ID,
	)
	if err != nil {
		t.Fatalf("get operation: %v", err)
	}

	if stored.RecoveryWorkerID == nil {
		t.Fatal("expected an assigned recovery worker")
	}

	if stored.RecoveryAttemptCount != 1 {
		t.Fatalf(
			"expected exactly one recovery attempt, got %d",
			stored.RecoveryAttemptCount,
		)
	}
}
