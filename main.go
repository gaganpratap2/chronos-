package main

import (
	"chronos/execution"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"chronos/database"
	"chronos/job"
	"chronos/worker"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

type CreateJobRequest struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

const createJobOperationScope = "POST /jobs"

func hashCreateJobRequest(request CreateJobRequest) ([]byte, error) {
	// Convert the validated request into a stable JSON representation.

	normalized, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf(
			"marshal request for hashing: %w",
			err,
		)
	}

	// Generate a SHA-256 fingerprint.
	hash := sha256.Sum256(normalized)

	return hash[:], nil
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, `{"status":"ok"}`)
}

func createJobHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(
				w,
				"method not allowed",
				http.StatusMethodNotAllowed,
			)
			return
		}

		// Step 1: Require an idempotency key.
		idempotencyKey := strings.TrimSpace(
			r.Header.Get("Idempotency-Key"),
		)

		if idempotencyKey == "" {
			http.Error(
				w,
				"Idempotency-Key header is required",
				http.StatusBadRequest,
			)
			return
		}

		if len(idempotencyKey) > 255 {
			http.Error(
				w,
				"Idempotency-Key must not exceed 255 bytes",
				http.StatusBadRequest,
			)
			return
		}

		// Step 2: Decode the request.
		var request CreateJobRequest

		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(
				w,
				"invalid JSON",
				http.StatusBadRequest,
			)
			return
		}

		// Step 3: Validate and construct the job.
		newJob, err := job.NewJob(
			request.ID,
			request.Name,
		)

		if err != nil {
			http.Error(
				w,
				err.Error(),
				http.StatusBadRequest,
			)
			return
		}

		// Step 4: Calculate the request fingerprint.
		requestHash, err := hashCreateJobRequest(request)
		if err != nil {
			log.Printf("Failed to hash job request: %v", err)

			http.Error(
				w,
				"failed to process request",
				http.StatusInternalServerError,
			)
			return
		}

		// Step 5: Generate the original HTTP response before
		// starting the database transaction.
		responseBody, err := json.Marshal(newJob)
		if err != nil {
			log.Printf("Failed to encode job response: %v", err)

			http.Error(
				w,
				"failed to encode job response",
				http.StatusInternalServerError,
			)
			return
		}

		// Step 6: Create the job idempotently.
		result, err := database.CreateJobIdempotently(
			r.Context(),
			pool,
			createJobOperationScope,
			idempotencyKey,
			requestHash,
			newJob,
			http.StatusCreated,
			"application/json",
			responseBody,
		)

		if err != nil {
			// Same key with a different request payload.
			if errors.Is(err, database.ErrIdempotencyConflict) {
				http.Error(
					w,
					"idempotency key reused with a different request",
					http.StatusConflict,
				)
				return
			}

			// Existing request is not yet complete.
			if errors.Is(err, database.ErrIdempotencyInProgress) {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(http.StatusAccepted)

				_, writeErr := w.Write(
					[]byte(`{"status":"processing"}`),
				)
				if writeErr != nil {
					log.Printf(
						"Failed to write in-progress response: %v",
						writeErr,
					)
				}
				return
			}

			// Invalid service input.
			if errors.Is(err, database.ErrInvalidIdempotencyInput) {
				http.Error(
					w,
					"invalid idempotency request",
					http.StatusBadRequest,
				)
				return
			}

			// Preserve the existing duplicate-job-ID behaviour.
			var pgErr *pgconn.PgError

			if errors.As(err, &pgErr) && pgErr.Code == "23505" {
				http.Error(
					w,
					"job ID already exists",
					http.StatusConflict,
				)
				return
			}

			// Fail closed if the database cannot verify or
			// complete the idempotent operation.
			log.Printf(
				"Failed to create job %d idempotently: %v",
				newJob.ID,
				err,
			)

			http.Error(
				w,
				"failed to create job",
				http.StatusInternalServerError,
			)
			return
		}

		// Step 7: Send either the newly created result
		// or the original stored response.
		w.Header().Set(
			"Content-Type",
			result.ContentType,
		)

		w.WriteHeader(result.HTTPStatus)

		if _, err := w.Write(result.Body); err != nil {
			log.Printf(
				"Failed to write job response: %v",
				err,
			)
		}
	}
}

func getJobHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		id, err := strconv.Atoi(r.PathValue("id"))
		if err != nil || id <= 0 {
			http.Error(w, "invalid job ID", http.StatusBadRequest)
			return
		}

		savedJob, err := database.GetJob(r.Context(), pool, id)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				http.Error(w, "job not found", http.StatusNotFound)
				return
			}

			log.Printf("Failed to retrieve job %d: %v", id, err)
			http.Error(w, "failed to retrieve job", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(savedJob); err != nil {
			log.Printf("Response encoding error: %v", err)
		}
	}
}

func transitionJobStatusHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		id, err := strconv.Atoi(r.PathValue("id"))
		if err != nil || id <= 0 {
			http.Error(w, "invalid job ID", http.StatusBadRequest)
			return
		}

		var request struct {
			Status       string `json:"status"`
			FencingToken int64  `json:"fencing_token"`
		}

		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}

		if request.Status == "" {
			http.Error(w, "status is required", http.StatusBadRequest)
			return
		}

		updatedJob, err := database.TransitionJobStatus(
			r.Context(),
			pool,
			id,
			request.Status,
			request.FencingToken,
		)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				http.Error(w, "job not found", http.StatusNotFound)
				return
			}

			if errors.Is(err, job.ErrInvalidTransition) {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			log.Printf("Failed to transition job %d: %v", id, err)
			http.Error(w, "failed to transition job", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(updatedJob); err != nil {
			log.Printf("Response encoding error: %v", err)
		}
	}
}

// type AppHandler struct{}

// func (h AppHandler) Execute(ctx context.Context, j job.Job) error {
// 	log.Printf("Executing job %d: %s", j.ID, j.Name)
// 	return nil
// }

type AppHandler struct{}

const (
	appJobOperationName = "execute-job"
	appJobProvider      = "development"
)

func (h AppHandler) Execute(
	ctx context.Context,
	j job.Job,
) error {
	// Standalone jobs retain their existing execution path.
	log.Printf("Executing standalone job %d: %s", j.ID, j.Name)
	return nil
}

func (h AppHandler) ExecuteWithOperations(
	ctx context.Context,
	j job.Job,
	execContext worker.OperationExecutionContext,
) error {
	if execContext.Operations == nil {
		return errors.New("operation executor is nil")
	}

	if execContext.WorkflowTaskID <= 0 {
		return errors.New("invalid workflow task ID")
	}

	if execContext.FencingToken <= 0 {
		return errors.New("invalid fencing token")
	}

	// Build a deterministic payload for this logical job operation.
	payload, err := json.Marshal(struct {
		JobID   int    `json:"job_id"`
		JobName string `json:"job_name"`
	}{
		JobID:   j.ID,
		JobName: j.Name,
	})
	if err != nil {
		return fmt.Errorf("encode job operation payload: %w", err)
	}

	// This key remains unchanged across retries of this workflow task.
	idempotencyKey := fmt.Sprintf(
		"chronos:workflow-task:%d:%s",
		execContext.WorkflowTaskID,
		appJobOperationName,
	)

	result, err := execContext.Operations.Execute(
		ctx,
		execution.OperationRequest{
			WorkflowTaskID: execContext.WorkflowTaskID,
			OperationName:  appJobOperationName,
			Provider:       appJobProvider,
			IdempotencyKey: idempotencyKey,
			FencingToken:   execContext.FencingToken,
			Payload:        payload,
		},
	)
	if err != nil {
		return fmt.Errorf(
			"execute operation for workflow task %d: %w",
			execContext.WorkflowTaskID,
			err,
		)
	}

	log.Printf(
		"Workflow task %d operation completed: provider_reference=%s replayed=%t",
		execContext.WorkflowTaskID,
		result.ProviderReference,
		result.Replayed,
	)

	return nil
}

var _ worker.OperationAwareHandler = AppHandler{}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println(".env not found; using system environment variables")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	pool, err := database.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	appHandler := AppHandler{}

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("failed to get hostname: %v", err)
	}

	developmentProvider, err := execution.NewDevelopmentProvider(pool)
	if err != nil {
		log.Fatalf("create development provider: %v", err)
	}

	operationService, err := execution.NewOperationService(
		pool,
		map[string]execution.Provider{
			appJobProvider: developmentProvider,
		},
	)
	if err != nil {
		log.Fatalf("create operation service: %v", err)
	}

	workerID := fmt.Sprintf("%s-%d", hostname, os.Getpid())
	log.Printf("Starting worker with ID: %s", workerID)

	w := &worker.Worker{
		WorkerID:          workerID,
		Pool:              pool,
		Handler:           appHandler,
		OperationExecutor: operationService,
		PollInterval:      500 * time.Millisecond,
	}

	workerCtx, workerCancel := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer workerCancel()

	go func() {
		if err := w.Run(workerCtx); err != nil {
			log.Printf("Worker stopped: %v", err)
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/jobs", createJobHandler(pool))
	mux.HandleFunc("/jobs/{id}", getJobHandler(pool))
	mux.HandleFunc(
		"PATCH /jobs/{id}/status",
		transitionJobStatusHandler(pool),
	)

	mux.HandleFunc(
		"PATCH /recurring-schedules/{id}/enabled",
		recurringScheduleEnabledHandler(pool),
	)
	mux.HandleFunc(
		"PATCH /recurring-schedules/{id}/interval",
		rescheduleIntervalHandler(pool),
	)
	mux.HandleFunc(
		"PATCH /recurring-schedules/{id}/cron",
		rescheduleCronHandler(pool),
	)

	fmt.Println("Chronos API listening on http://localhost:8080")

	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil &&
			!errors.Is(err, http.ErrServerClosed) {
			log.Printf("HTTP server error: %v", err)
			workerCancel()
		}
	}()

	<-workerCtx.Done()

	shutdownCtx, shutdownCancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}
}
