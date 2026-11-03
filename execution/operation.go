package execution

import (
	"context"
)

type OperationRequest struct {
	WorkflowTaskID int64
	OperationName  string
	Provider       string
	IdempotencyKey string
	FencingToken   int64
	Payload        []byte
}

type OperationResult struct {
	ProviderReference string
	Data              []byte
	Replayed          bool
}

type OperationExecutor interface {
	Execute(ctx context.Context, request OperationRequest) (OperationResult, error)
}
