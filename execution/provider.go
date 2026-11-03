package execution

import (
	"context"
)

type ProviderOutcome string

const (
	ProviderSucceeded ProviderOutcome = "succeeded"
	ProviderFailed    ProviderOutcome = "failed"
	ProviderPending   ProviderOutcome = "pending"
	ProviderNotFound  ProviderOutcome = "not_found"
	ProviderUnknown   ProviderOutcome = "unknown"
)

type ProviderResult struct {
	Outcome           ProviderOutcome
	ProviderReference string
	Data              []byte
}

type Provider interface {
	Execute(ctx context.Context, idempotencyKey string, payload []byte) (ProviderResult, error)

	Lookup(ctx context.Context, idempotencyKey string) (ProviderResult, error)
}
