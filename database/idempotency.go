package database

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"chronos/job"

	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrIdempotencyConflict = errors.New(
		"idempotency key reused with a different request",
	)

	ErrIdempotencyInProgress = errors.New(
		"idempotent request is still processing",
	)

	ErrInvalidIdempotencyInput = errors.New(
		"invalid idempotency input",
	)
)

type IdempotencyResult struct {
	HTTPStatus  int
	ContentType string
	Body        []byte
	Replayed    bool
}

func CreateJobIdempotently(ctx context.Context, pool *pgxpool.Pool, operationScope string, idempotencyKey string, requestHash []byte, newJob job.Job, responseStatus int, responseContentType string, responseBody []byte) (IdempotencyResult, error) {

	// Validate the inputs before opening a transaction.

	if pool == nil ||
		strings.TrimSpace(operationScope) == "" ||
		strings.TrimSpace(idempotencyKey) == "" ||
		utf8.RuneCountInString(idempotencyKey) > 255 ||
		len(requestHash) != 32 ||
		newJob.ID <= 0 ||
		newJob.Name == "" ||
		responseStatus < 100 ||
		responseStatus > 599 ||
		strings.TrimSpace(responseContentType) == "" ||
		responseBody == nil {
		return IdempotencyResult{}, ErrInvalidIdempotencyInput
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return IdempotencyResult{}, fmt.Errorf(
			"begin idempotency transaction: %w",
			err,
		)
	}

	defer func() {
		_ = tx.Rollback(ctx)
	}()

	// Step 1: Reserve the idempotency key.
	// If another transaction already owns this key, no new row is inserted.
	insertResult, err := tx.Exec(ctx,
		`INSERT INTO idempotency_records (
		operation_scope,
		idempotency_key,
		request_hash,
		status
	)
	VALUES ($1 , $2 , $3 , 'processing')
	ON CONFLICT (operation_scope , idempotency_key)
	DO NOTHING`,
		operationScope, idempotencyKey, requestHash,
	)

	if err != nil {
		return IdempotencyResult{}, fmt.Errorf(
			"reserve idempotency key: %w",
			err,
		)
	}

	// Step 2: An existing key was found.
	if insertResult.RowsAffected() == 0 {
		var storedHash []byte
		var storedStatus string

		err := tx.QueryRow(ctx,
			`SELECT request_hash , status
		FROM idempotency_records
		WHERE operation_scope = $1 
			AND idempotency_key = $2`,
			operationScope,
			idempotencyKey,
		).Scan(
			&storedHash, &storedStatus)

		if err != nil {
			return IdempotencyResult{}, fmt.Errorf(
				"retrieve existing idempotency record: %w",
				err,
			)
		}

		// The same key cannot be reused for different request data.
		if !bytes.Equal(storedHash, requestHash) {
			return IdempotencyResult{}, ErrIdempotencyConflict
		}

		if storedStatus == "processing" {
			return IdempotencyResult{}, ErrIdempotencyInProgress
		}

		if storedStatus != "completed" {
			return IdempotencyResult{}, fmt.Errorf(
				"unexpected idempotency status: %s",
				storedStatus,
			)
		}

		// Retrieve the original HTTP response.
		var result IdempotencyResult

		err = tx.QueryRow(
			ctx,
			`SELECT
        response_status,
        response_content_type,
        response_body
    FROM idempotency_records
    WHERE operation_scope = $1
      AND idempotency_key = $2`,
			operationScope,
			idempotencyKey,
		).Scan(
			&result.HTTPStatus,
			&result.ContentType,
			&result.Body,
		)

		if err != nil {
			return IdempotencyResult{}, fmt.Errorf(
				"retrieve original response: %w",
				err,
			)
		}

		result.Replayed = true
		return result, nil
	}

	// its a new request
	_, err = tx.Exec(ctx,
		`INSERT INTO jobs (
			id,
			name,
			status,
			run_at,
			retry_count,
			max_retries,
			next_retry_at,
			worker_id,
			lease_until,
			fencing_token
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		newJob.ID,
		newJob.Name,
		newJob.Status,
		newJob.RunAt,
		newJob.RetryCount,
		newJob.MaxRetries,
		newJob.NextRetryAt,
		newJob.WorkerID,
		newJob.LeaseUntil,
		newJob.FencingToken,
	)

	if err != nil {
		return IdempotencyResult{}, fmt.Errorf(
			"insert job: %w",
			err,
		)
	}

	// complete the idempotency  recored

	_, err = tx.Exec(ctx,
		`UPDATE idempotency_records
		SET status = 'completed',
			job_id = $3,
			response_status = $4,
			response_content_type = $5,
			response_body = $6
		WHERE operation_scope = $1
		  AND idempotency_key = $2
		  AND status = 'processing'`,
		operationScope,
		idempotencyKey,
		newJob.ID,
		responseStatus,
		responseContentType,
		responseBody,
	)

	if err != nil {
		return IdempotencyResult{}, fmt.Errorf(
			"complete idempotency record: %w",
			err,
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return IdempotencyResult{}, fmt.Errorf(
			"commit idempotent job creation: %w",
			err,
		)
	}

	return IdempotencyResult{
		HTTPStatus:  responseStatus,
		ContentType: responseContentType,
		Body:        responseBody,
		Replayed:    false,
	}, nil
}
