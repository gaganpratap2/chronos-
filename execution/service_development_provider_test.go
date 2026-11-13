package execution

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"chronos/database"
	"chronos/observability"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

type developmentProviderIntegrationFixture struct {
	pool         *pgxpool.Pool
	ctx          context.Context
	taskID       int64
	jobID        int
	fencingToken int64
}

// responseLostDevelopmentProvider simulates an external provider
// that successfully performs an operation but loses its response
// before Chronos receives it.
type responseLostDevelopmentProvider struct {
	provider *DevelopmentProvider

	executeCalls int
	lookupCalls  int
}

func (p *responseLostDevelopmentProvider) Execute(
	ctx context.Context,
	idempotencyKey string,
	payload []byte,
) (ProviderResult, error) {
	p.executeCalls++

	// The underlying provider performs the operation and persists
	// its receipt successfully.
	_, err := p.provider.Execute(
		ctx,
		idempotencyKey,
		payload,
	)
	if err != nil {
		return ProviderResult{}, err
	}

	// Simulate the network failing after provider success.
	// Chronos never receives the successful response.
	return ProviderResult{}, errors.New(
		"simulated network failure after provider execution",
	)
}

func (p *responseLostDevelopmentProvider) Lookup(
	ctx context.Context,
	idempotencyKey string,
) (ProviderResult, error) {
	p.lookupCalls++

	// The replacement worker queries the provider using the
	// original idempotency key.
	return p.provider.Lookup(ctx, idempotencyKey)
}

func newDevelopmentProviderIntegrationFixture(
	t *testing.T,
) *developmentProviderIntegrationFixture {
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

	rollback := func() {
		_ = tx.Rollback(ctx)
		pool.Close()
		cancel()
	}

	// Create the parent workflow.
	_, err = tx.Exec(
		ctx,
		`INSERT INTO workflows (
			id,
			name,
			status
		)
		VALUES ($1, $2, $3)`,
		workflowID,
		"development-provider-integration-test",
		"Running",
	)
	if err != nil {
		rollback()
		t.Fatalf("insert fixture workflow: %v", err)
	}

	// Create a running job with a valid fencing token.
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
		VALUES (
			$1,
			$2,
			'Running',
			0,
			3,
			$3,
			NOW() + INTERVAL '30 minutes',
			$4
		)`,
		jobID,
		"development-provider-integration-test",
		"development-provider-test-worker",
		fencingToken,
	)
	if err != nil {
		rollback()
		t.Fatalf("insert fixture job: %v", err)
	}

	// Link the workflow task to the running job.
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
		VALUES (
			$1,
			$2,
			$3,
			$4,
			'Running',
			NOW(),
			NOW()
		)`,
		taskID,
		workflowID,
		jobID,
		"development-provider-integration-test",
	)
	if err != nil {
		rollback()
		t.Fatalf("insert fixture workflow task: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		pool.Close()
		cancel()
		t.Fatalf("commit fixture transaction: %v", err)
	}

	fixture := &developmentProviderIntegrationFixture{
		pool:         pool,
		ctx:          ctx,
		taskID:       taskID,
		jobID:        jobID,
		fencingToken: fencingToken,
	}

	// Clean up all records created by this test.
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cleanupCancel()

		idempotencyKey := fmt.Sprintf(
			"chronos:integration-test:%d",
			taskID,
		)

		// Remove the development provider receipt.
		_, err := pool.Exec(
			cleanupCtx,
			`DELETE FROM development_provider_receipts
			 WHERE idempotency_key = $1`,
			idempotencyKey,
		)
		if err != nil {
			t.Errorf("cleanup development provider receipt: %v", err)
		}

		// Remove execution operations before their referenced task.
		_, err = pool.Exec(
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
			`DELETE FROM workflow_tasks
			 WHERE id = $1`,
			taskID,
		)
		if err != nil {
			t.Errorf("cleanup workflow task: %v", err)
		}

		_, err = pool.Exec(
			cleanupCtx,
			`DELETE FROM jobs
			 WHERE id = $1`,
			jobID,
		)
		if err != nil {
			t.Errorf("cleanup job: %v", err)
		}

		_, err = pool.Exec(
			cleanupCtx,
			`DELETE FROM workflows
			 WHERE id = $1`,
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

// Test the complete OperationService -> DevelopmentProvider path.
//
// The first execution must create a provider receipt.
// The second execution must replay the stored result without
// creating another provider receipt or execution record.
func TestOperationServiceWithDevelopmentProvider(t *testing.T) {
	f := newDevelopmentProviderIntegrationFixture(t)

	// Initialize the development provider.
	provider, err := NewDevelopmentProvider(f.pool)
	if err != nil {
		t.Fatalf("create development provider: %v", err)
	}

	registry := prometheus.NewRegistry()

	metrics, err := observability.NewMetrics(registry)
	if err != nil {
		t.Fatalf("create metrics: %v", err)
	}

	operationService, err := NewOperationService(
		f.pool,
		map[string]Provider{
			"development": provider,
		},
		metrics,
	)

	if err != nil {
		t.Fatalf("create operation service: %v", err)
	}

	payload := []byte(`{"amount":1500,"currency":"INR"}`)

	idempotencyKey := fmt.Sprintf(
		"chronos:integration-test:%d",
		f.taskID,
	)

	request := OperationRequest{
		WorkflowTaskID: f.taskID,
		OperationName:  "integration-test",
		Provider:       "development",
		IdempotencyKey: idempotencyKey,
		FencingToken:   f.fencingToken,
		Payload:        payload,
	}

	// Execute the operation for the first time.
	firstResult, err := operationService.Execute(
		f.ctx,
		request,
	)
	if err != nil {
		t.Fatalf("first execution failed: %v", err)
	}

	if firstResult.Replayed {
		t.Fatal("first execution should not be a replay")
	}

	if firstResult.ProviderReference == "" {
		t.Fatal("first execution returned an empty provider reference")
	}

	if len(firstResult.Data) == 0 {
		t.Fatal("first execution returned empty result data")
	}

	// Execute the exact same logical operation again.
	secondResult, err := operationService.Execute(
		f.ctx,
		request,
	)
	if err != nil {
		t.Fatalf("second execution failed: %v", err)
	}

	if !secondResult.Replayed {
		t.Fatal("second execution should replay the stored result")
	}

	if firstResult.ProviderReference != secondResult.ProviderReference {
		t.Fatalf(
			"provider reference changed during replay: first=%q second=%q",
			firstResult.ProviderReference,
			secondResult.ProviderReference,
		)
	}

	if !bytes.Equal(firstResult.Data, secondResult.Data) {
		t.Fatalf(
			"result data changed during replay: first=%s second=%s",
			firstResult.Data,
			secondResult.Data,
		)
	}

	// Verify the execution operation was stored exactly once.
	var operationCount int64
	var operationStatus string
	var attemptCount int

	err = f.pool.QueryRow(
		f.ctx,
		`SELECT
			COUNT(*),
			COALESCE(MAX(status), ''),
			COALESCE(MAX(attempt_count), 0)
		FROM execution_operations
		WHERE workflow_task_id = $1
		  AND operation_name = $2`,
		f.taskID,
		"integration-test",
	).Scan(
		&operationCount,
		&operationStatus,
		&attemptCount,
	)
	if err != nil {
		t.Fatalf("query execution operation: %v", err)
	}

	if operationCount != 1 {
		t.Fatalf(
			"expected exactly one execution operation, got %d",
			operationCount,
		)
	}

	if operationStatus != "succeeded" {
		t.Fatalf(
			"expected execution operation status succeeded, got %q",
			operationStatus,
		)
	}

	if attemptCount != 1 {
		t.Fatalf(
			"expected operation attempt count 1, got %d",
			attemptCount,
		)
	}

	// Verify the development provider created exactly one receipt.
	var receiptCount int64
	var receiptReference string

	err = f.pool.QueryRow(
		f.ctx,
		`SELECT
			COUNT(*),
			COALESCE(MAX(provider_reference), '')
		FROM development_provider_receipts
		WHERE idempotency_key = $1`,
		idempotencyKey,
	).Scan(
		&receiptCount,
		&receiptReference,
	)
	if err != nil {
		t.Fatalf("query development provider receipt: %v", err)
	}

	if receiptCount != 1 {
		t.Fatalf(
			"expected exactly one provider receipt, got %d",
			receiptCount,
		)
	}

	if receiptReference != firstResult.ProviderReference {
		t.Fatalf(
			"receipt reference mismatch: expected %q, got %q",
			firstResult.ProviderReference,
			receiptReference,
		)
	}

	metricFamilies, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}

	var observationCount uint64

	for _, family := range metricFamilies {
		if family.GetName() == "chronos_external_operation_duration_seconds" {
			observationCount = family.GetMetric()[0].
				GetHistogram().
				GetSampleCount()
			break
		}
	}

	if observationCount != 1 {
		t.Fatalf(
			"expected one external operation duration observation, got %d",
			observationCount,
		)
	}
}

func TestOperationServiceRecoversAfterProviderResponseLoss(
	t *testing.T,
) {

	registry := prometheus.NewRegistry()

	metrics, err := observability.NewMetrics(registry)
	if err != nil {
		t.Fatalf("create metrics: %v", err)
	}

	f := newDevelopmentProviderIntegrationFixture(t)

	// Create the underlying development provider.
	baseProvider, err := NewDevelopmentProvider(f.pool)
	if err != nil {
		t.Fatalf("create development provider: %v", err)
	}

	// Wrap it with a simulated network failure.
	provider := &responseLostDevelopmentProvider{
		provider: baseProvider,
	}

	// Register the simulated provider.
	operationService, err := NewOperationService(
		f.pool,
		map[string]Provider{
			"development": provider,
		},
		metrics,
	)
	if err != nil {
		t.Fatalf("create operation service: %v", err)
	}

	idempotencyKey := fmt.Sprintf(
		"chronos:integration-test:%d",
		f.taskID,
	)

	request := OperationRequest{
		WorkflowTaskID: f.taskID,
		OperationName:  "response-loss-test",
		Provider:       "development",
		IdempotencyKey: idempotencyKey,
		FencingToken:   f.fencingToken,
		Payload: []byte(
			`{"amount":2500,"currency":"INR"}`,
		),
	}

	// ---------------------------------------------------------
	// Stage 1: Worker A executes the operation.
	// ---------------------------------------------------------

	_, err = operationService.Execute(
		f.ctx,
		request,
	)

	if err == nil {
		t.Fatal("expected a simulated response-loss error")
	}

	if provider.executeCalls != 1 {
		t.Fatalf(
			"expected one provider execution, got %d",
			provider.executeCalls,
		)
	}

	if provider.lookupCalls != 0 {
		t.Fatalf(
			"expected no lookup before recovery, got %d",
			provider.lookupCalls,
		)
	}

	// Verify that the provider actually completed the operation.
	var originalProviderReference string

	err = f.pool.QueryRow(
		f.ctx,
		`SELECT provider_reference
		 FROM development_provider_receipts
		 WHERE idempotency_key = $1`,
		idempotencyKey,
	).Scan(&originalProviderReference)

	if err != nil {
		t.Fatalf(
			"retrieve original provider receipt: %v",
			err,
		)
	}

	if originalProviderReference == "" {
		t.Fatal("provider did not persist its receipt")
	}

	// Chronos should record the result as uncertain.
	var status string

	err = f.pool.QueryRow(
		f.ctx,
		`SELECT status
		 FROM execution_operations
		 WHERE workflow_task_id = $1
		   AND operation_name = $2`,
		f.taskID,
		"response-loss-test",
	).Scan(&status)

	if err != nil {
		t.Fatalf("retrieve execution operation status: %v", err)
	}

	if status != "outcome_unknown" {
		t.Fatalf(
			"expected outcome_unknown, got %q",
			status,
		)
	}

	// ---------------------------------------------------------
	// Stage 2: Worker B takes ownership of the job.
	// ---------------------------------------------------------

	const newFencingToken int64 = 8

	tag, err := f.pool.Exec(
		f.ctx,
		`UPDATE jobs
		 SET fencing_token = $2,
		     worker_id = 'response-recovery-worker-B',
		     lease_until = NOW() + INTERVAL '30 minutes'
		 WHERE id = $1
		   AND fencing_token = $3
		   AND status = 'Running'`,
		f.jobID,
		newFencingToken,
		f.fencingToken,
	)

	if err != nil {
		t.Fatalf("transfer job ownership: %v", err)
	}

	if tag.RowsAffected() != 1 {
		t.Fatal("expected job ownership to transfer to Worker B")
	}

	// Worker B uses the same logical request and idempotency key,
	// but its newly acquired fencing token.
	request.FencingToken = newFencingToken

	// ---------------------------------------------------------
	// Stage 3: Worker B reconciles the uncertain operation.
	// ---------------------------------------------------------

	recoveredResult, err := operationService.Execute(
		f.ctx,
		request,
	)

	if err != nil {
		t.Fatalf(
			"recover operation after response loss: %v",
			err,
		)
	}

	if recoveredResult.ProviderReference != originalProviderReference {
		t.Fatalf(
			"expected original provider reference %q, got %q",
			originalProviderReference,
			recoveredResult.ProviderReference,
		)
	}

	// The provider must have been queried instead of executed again.
	if provider.executeCalls != 1 {
		t.Fatalf(
			"expected exactly one provider execution, got %d",
			provider.executeCalls,
		)
	}

	if provider.lookupCalls != 1 {
		t.Fatalf(
			"expected one provider lookup, got %d",
			provider.lookupCalls,
		)
	}

	// ---------------------------------------------------------
	// Stage 4: Verify final persisted state.
	// ---------------------------------------------------------

	var operationCount int64
	var finalStatus string
	var attemptCount int
	var savedFencingToken int64

	err = f.pool.QueryRow(
		f.ctx,
		`SELECT
			COUNT(*),
			COALESCE(MAX(status), ''),
			COALESCE(MAX(attempt_count), 0),
			COALESCE(MAX(fencing_token), 0)
		 FROM execution_operations
		 WHERE workflow_task_id = $1
		   AND operation_name = $2`,
		f.taskID,
		"response-loss-test",
	).Scan(
		&operationCount,
		&finalStatus,
		&attemptCount,
		&savedFencingToken,
	)

	if err != nil {
		t.Fatalf("verify recovered operation: %v", err)
	}

	if operationCount != 1 {
		t.Fatalf(
			"expected one execution operation, got %d",
			operationCount,
		)
	}

	if finalStatus != "succeeded" {
		t.Fatalf(
			"expected final status succeeded, got %q",
			finalStatus,
		)
	}

	if attemptCount != 2 {
		t.Fatalf(
			"expected attempt count 2 after takeover, got %d",
			attemptCount,
		)
	}

	if savedFencingToken != newFencingToken {
		t.Fatalf(
			"expected fencing token %d, got %d",
			newFencingToken,
			savedFencingToken,
		)
	}

	// There must still be exactly one provider receipt.
	var receiptCount int64

	err = f.pool.QueryRow(
		f.ctx,
		`SELECT COUNT(*)
		 FROM development_provider_receipts
		 WHERE idempotency_key = $1`,
		idempotencyKey,
	).Scan(&receiptCount)

	if err != nil {
		t.Fatalf("verify provider receipt count: %v", err)
	}

	if receiptCount != 1 {
		t.Fatalf(
			"expected exactly one provider receipt, got %d",
			receiptCount,
		)
	}

	metricFamilies, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}

	var observationCount uint64

	for _, family := range metricFamilies {
		if family.GetName() == "chronos_external_operation_duration_seconds" {
			observationCount = family.GetMetric()[0].
				GetHistogram().
				GetSampleCount()
			break
		}
	}

	if observationCount != 2 {
		t.Fatalf(
			"expected two external operation duration observations (Execute + Lookup), got %d",
			observationCount,
		)
	}

}
