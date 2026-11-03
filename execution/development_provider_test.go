package execution

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"chronos/database"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

type developmentProviderFixture struct {
	ctx            context.Context
	pool           *pgxpool.Pool
	provider       *DevelopmentProvider
	idempotencyKey string
}

func newDevelopmentProviderFixture(
	t *testing.T,
) *developmentProviderFixture {
	t.Helper()

	if err := godotenv.Load("../.env"); err != nil {
		t.Log(".env not found; using system environment variables")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)

	pool, err := database.Connect(ctx)
	if err != nil {
		cancel()
		t.Fatalf("connect database: %v", err)
	}

	provider, err := NewDevelopmentProvider(pool)
	if err != nil {
		pool.Close()
		cancel()
		t.Fatalf("create development provider: %v", err)
	}

	key := fmt.Sprintf(
		"development-provider-test-%d",
		time.Now().UnixNano(),
	)

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cleanupCancel()

		_, err := pool.Exec(
			cleanupCtx,
			`DELETE FROM development_provider_receipts
			 WHERE idempotency_key = $1`,
			key,
		)
		if err != nil {
			t.Errorf("cleanup provider receipt: %v", err)
		}

		pool.Close()
		cancel()
	})

	return &developmentProviderFixture{
		ctx:            ctx,
		pool:           pool,
		provider:       provider,
		idempotencyKey: key,
	}
}

func TestDevelopmentProviderExecuteAndReplay(t *testing.T) {
	f := newDevelopmentProviderFixture(t)

	payload := []byte(`{"job_id":123,"name":"send-email"}`)

	first, err := f.provider.Execute(
		f.ctx,
		f.idempotencyKey,
		payload,
	)
	if err != nil {
		t.Fatalf("first execution: %v", err)
	}

	if first.Outcome != ProviderSucceeded {
		t.Fatalf("expected ProviderSucceeded, got %s", first.Outcome)
	}

	second, err := f.provider.Execute(
		f.ctx,
		f.idempotencyKey,
		payload,
	)
	if err != nil {
		t.Fatalf("repeated execution: %v", err)
	}

	if second.Outcome != ProviderSucceeded {
		t.Fatalf("expected ProviderSucceeded, got %s", second.Outcome)
	}

	if first.ProviderReference != second.ProviderReference {
		t.Fatal("provider reference changed during replay")
	}

	if string(first.Data) != string(second.Data) {
		t.Fatal("provider result changed during replay")
	}

	var receiptCount int

	err = f.pool.QueryRow(
		f.ctx,
		`SELECT COUNT(*)
		 FROM development_provider_receipts
		 WHERE idempotency_key = $1`,
		f.idempotencyKey,
	).Scan(&receiptCount)
	if err != nil {
		t.Fatalf("count provider receipts: %v", err)
	}

	if receiptCount != 1 {
		t.Fatalf("expected one receipt, got %d", receiptCount)
	}
}

func TestDevelopmentProviderRejectsChangedPayload(t *testing.T) {
	f := newDevelopmentProviderFixture(t)

	firstPayload := []byte(`{"amount":1000,"currency":"INR"}`)
	changedPayload := []byte(`{"amount":2000,"currency":"INR"}`)

	_, err := f.provider.Execute(
		f.ctx,
		f.idempotencyKey,
		firstPayload,
	)
	if err != nil {
		t.Fatalf("first execution: %v", err)
	}

	_, err = f.provider.Execute(
		f.ctx,
		f.idempotencyKey,
		changedPayload,
	)

	if !errors.Is(err, ErrDevelopmentProviderKeyConflict) {
		t.Fatalf(
			"expected ErrDevelopmentProviderKeyConflict, got %v",
			err,
		)
	}

	var receiptCount int

	err = f.pool.QueryRow(
		f.ctx,
		`SELECT COUNT(*)
		 FROM development_provider_receipts
		 WHERE idempotency_key = $1`,
		f.idempotencyKey,
	).Scan(&receiptCount)
	if err != nil {
		t.Fatalf("count provider receipts: %v", err)
	}

	if receiptCount != 1 {
		t.Fatalf(
			"expected original receipt to remain unchanged, got %d receipts",
			receiptCount,
		)
	}
}

func TestDevelopmentProviderLookup(t *testing.T) {
	f := newDevelopmentProviderFixture(t)

	// A key that has not been executed must not appear successful.
	missing, err := f.provider.Lookup(
		f.ctx,
		f.idempotencyKey,
	)
	if err != nil {
		t.Fatalf("lookup missing receipt: %v", err)
	}

	if missing.Outcome != ProviderNotFound {
		t.Fatalf(
			"expected ProviderNotFound, got %s",
			missing.Outcome,
		)
	}

	payload := []byte(`{"job_id":456,"name":"process-payment"}`)

	executed, err := f.provider.Execute(
		f.ctx,
		f.idempotencyKey,
		payload,
	)
	if err != nil {
		t.Fatalf("execute operation: %v", err)
	}

	// Simulate recovery: retrieve the original receipt without
	// executing the operation again.
	recovered, err := f.provider.Lookup(
		f.ctx,
		f.idempotencyKey,
	)
	if err != nil {
		t.Fatalf("lookup existing receipt: %v", err)
	}

	if recovered.Outcome != ProviderSucceeded {
		t.Fatalf(
			"expected ProviderSucceeded, got %s",
			recovered.Outcome,
		)
	}

	if recovered.ProviderReference != executed.ProviderReference {
		t.Fatal("recovered provider reference does not match original")
	}

	if string(recovered.Data) != string(executed.Data) {
		t.Fatal("recovered result does not match original result")
	}
}

func TestNewDevelopmentProviderRejectsNilPool(t *testing.T) {
	provider, err := NewDevelopmentProvider(nil)

	if err == nil {
		t.Fatal("expected error for nil database pool")
	}

	if provider != nil {
		t.Fatal("expected provider to be nil")
	}
}
