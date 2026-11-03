package execution

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrDevelopmentProviderUnavailable = errors.New(
		"development provider database is unavailable",
	)

	ErrDevelopmentProviderKeyConflict = errors.New(
		"development provider idempotency key reused with different payload",
	)

	ErrDevelopmentProviderInvalidKey = errors.New(
		"development provider idempotency key is empty",
	)
)

type DevelopmentProvider struct {
	pool *pgxpool.Pool
}

func NewDevelopmentProvider(
	pool *pgxpool.Pool,
) (*DevelopmentProvider, error) {
	if pool == nil {
		return nil, ErrDevelopmentProviderUnavailable
	}

	return &DevelopmentProvider{
		pool: pool,
	}, nil
}

func (p *DevelopmentProvider) Execute(
	ctx context.Context,
	idempotencyKey string,
	payload []byte,
) (ProviderResult, error) {
	if p == nil || p.pool == nil {
		return ProviderResult{}, ErrDevelopmentProviderUnavailable
	}

	if strings.TrimSpace(idempotencyKey) == "" {
		return ProviderResult{}, ErrDevelopmentProviderInvalidKey
	}

	// Calculate the fingerprint of the original request.
	requestHash := sha256.Sum256(payload)

	// Generate a deterministic provider reference.
	keyHash := sha256.Sum256([]byte(idempotencyKey))
	providerReference := "dev-" + hex.EncodeToString(keyHash[:])

	resultData, err := json.Marshal(struct {
		Status      string `json:"status"`
		Provider    string `json:"provider"`
		PayloadHash string `json:"payload_hash"`
	}{
		Status:      "accepted",
		Provider:    "development",
		PayloadHash: hex.EncodeToString(requestHash[:]),
	})
	if err != nil {
		return ProviderResult{}, fmt.Errorf(
			"encode development provider result: %w",
			err,
		)
	}

	// Insert only if this idempotency key has no receipt.
	// A concurrent duplicate cannot create another receipt.
	_, err = p.pool.Exec(
		ctx,
		`INSERT INTO development_provider_receipts (
			idempotency_key,
			request_hash,
			provider_reference,
			result
		)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (idempotency_key) DO NOTHING`,
		idempotencyKey,
		requestHash[:],
		providerReference,
		resultData,
	)
	if err != nil {
		return ProviderResult{}, fmt.Errorf(
			"save development provider receipt: %w",
			err,
		)
	}

	// Retrieve the existing or newly created receipt.
	var (
		storedHash []byte
		storedRef  string
		storedData []byte
	)

	err = p.pool.QueryRow(
		ctx,
		`SELECT request_hash, provider_reference, result
		 FROM development_provider_receipts
		 WHERE idempotency_key = $1`,
		idempotencyKey,
	).Scan(
		&storedHash,
		&storedRef,
		&storedData,
	)
	if err != nil {
		return ProviderResult{}, fmt.Errorf(
			"retrieve development provider receipt: %w",
			err,
		)
	}

	// The same key must always represent the same request.
	if len(storedHash) != len(requestHash) ||
		!equalBytes(storedHash, requestHash[:]) {
		return ProviderResult{}, ErrDevelopmentProviderKeyConflict
	}

	return ProviderResult{
		Outcome:           ProviderSucceeded,
		ProviderReference: storedRef,
		Data:              storedData,
	}, nil
}

func (p *DevelopmentProvider) Lookup(
	ctx context.Context,
	idempotencyKey string,
) (ProviderResult, error) {
	if p == nil || p.pool == nil {
		return ProviderResult{}, ErrDevelopmentProviderUnavailable
	}

	if strings.TrimSpace(idempotencyKey) == "" {
		return ProviderResult{}, ErrDevelopmentProviderInvalidKey
	}

	var (
		providerReference string
		resultData        []byte
	)

	err := p.pool.QueryRow(
		ctx,
		`SELECT provider_reference, result
		 FROM development_provider_receipts
		 WHERE idempotency_key = $1`,
		idempotencyKey,
	).Scan(
		&providerReference,
		&resultData,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return ProviderResult{
			Outcome: ProviderNotFound,
		}, nil
	}

	if err != nil {
		return ProviderResult{}, fmt.Errorf(
			"lookup development provider receipt: %w",
			err,
		)
	}

	return ProviderResult{
		Outcome:           ProviderSucceeded,
		ProviderReference: providerReference,
		Data:              resultData,
	}, nil
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}
