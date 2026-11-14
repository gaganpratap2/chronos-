package main

import (
	"chronos/execution"
	"chronos/observability"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	// "log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"chronos/database"
	"chronos/job"
	"chronos/worker"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus"
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

func readinessHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		if pool == nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprintln(w, `{"status":"not_ready","reason":"database unavailable"}`)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		if err := pool.Ping(ctx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprintln(w, `{"status":"not_ready","reason":"database unavailable"}`)
			return
		}

		fmt.Fprintln(w, `{"status":"ready"}`)
	}
}

func createJobHandler(pool *pgxpool.Pool, metrics *observability.Metrics) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Step 1: Require an idempotency key.
		idempotencyKey := strings.TrimSpace(
			r.Header.Get("Idempotency-Key"),
		)

		if idempotencyKey == "" {
			http.Error(w, "Idempotency-Key header is required", http.StatusBadRequest)
			return
		}

		if len(idempotencyKey) > 255 {
			http.Error(w, "Idempotency-Key must not exceed 255 bytes", http.StatusBadRequest)
			return
		}

		// Step 2: Decode the request.
		var request CreateJobRequest

		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}

		// Step 3: Validate and construct the job.
		newJob, err := job.NewJob(
			request.ID,
			request.Name,
		)

		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// Step 4: Calculate the request fingerprint.
		requestHash, err := hashCreateJobRequest(request)
		if err != nil {
			slog.Error(
				"failed to hash job request",
				"error", err,
			)

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
			slog.Error(
				"failed to encode job response",
				"job_id", newJob.ID,
				"error", err,
			)

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
					slog.Warn(
						"failed to write in-progress response",
						"job_id", newJob.ID,
						"error", writeErr,
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
			slog.Error(
				"failed to create job idempotently",
				"job_id", newJob.ID,
				"error", err,
			)

			http.Error(
				w,
				"failed to create job",
				http.StatusInternalServerError,
			)
			return
		}

		if !result.Replayed {
			metrics.JobsCreated.Inc()
		}

		// Step 7: Send either the newly created result
		// or the original stored response.
		w.Header().Set(
			"Content-Type",
			result.ContentType,
		)

		w.WriteHeader(result.HTTPStatus)

		if _, err := w.Write(result.Body); err != nil {
			slog.Warn(
				"failed to write job response",
				"job_id", newJob.ID,
				"error", err,
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

			slog.Error(
				"failed to retrieve job",
				"job_id", id,
				"error", err,
			)
			http.Error(w, "failed to retrieve job", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(savedJob); err != nil {
			slog.Warn(
				"failed to encode job response",
				"job_id", id,
				"error", err,
			)
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

			slog.Error(
				"failed to transition job status",
				"job_id", id,
				"requested_status", request.Status,
				"error", err,
			)
			http.Error(w, "failed to transition job", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(updatedJob); err != nil {
			slog.Warn(
				"failed to encode updated job response",
				"job_id", id,
				"error", err,
			)
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
	slog.Info(
		"executing standalone job",
		"job_id", j.ID,
		"job_name", j.Name,
	)
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

	slog.Info(
		"workflow task operation completed",
		"workflow_task_id", execContext.WorkflowTaskID,
		"provider_reference", result.ProviderReference,
		"replayed", result.Replayed,
	)

	return nil
}

var _ worker.OperationAwareHandler = AppHandler{}

func main() {
	// if err := godotenv.Load(); err != nil {
	// 	if err := godotenv.Load(); err != nil {
	// 		slog.Info(
	// 			"dotenv file not loaded; using system environment variables",
	// 			"error", err,
	// 		)
	// 	}
	// }

	if err := godotenv.Load(); err != nil {
		slog.Info(
			"dotenv file not loaded; using system environment variables",
			"error", err,
		)
	}

	level, err := observability.ParseLevel(
		os.Getenv("CHRONOS_LOG_LEVEL"),
	)
	if err != nil {
		slog.Error("configure logger", "error", err)
		os.Exit(1)
	}

	logger := observability.NewJSONLogger(os.Stdout, level)
	slog.SetDefault(logger)

	slog.Info(
		"structured logger initialized",
		"log_level", level.String(),
	)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	pool, err := database.Connect(ctx)
	if err != nil {
		slog.Error("database connection failed", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	appHandler := AppHandler{}

	registry := prometheus.NewRegistry()

	metrics, err := observability.NewMetrics(registry)
	if err != nil {
		slog.Error("failed to initialize Prometheus metrics", "error", err)
		os.Exit(1)
	}

	hostname, err := os.Hostname()
	if err != nil {
		slog.Error("failed to get hostname", "error", err)
		os.Exit(1)
	}

	developmentProvider, err := execution.NewDevelopmentProvider(pool)
	if err != nil {
		slog.Error("failed to create development provider", "error", err)
		os.Exit(1)
	}

	operationService, err := execution.NewOperationService(
		pool,
		map[string]execution.Provider{
			appJobProvider: developmentProvider,
		},
		metrics,
	)
	if err != nil {
		slog.Error("failed to create operation service", "error", err)
		os.Exit(1)
	}

	workerID := fmt.Sprintf("%s-%d", hostname, os.Getpid())
	slog.Info(
		"worker started",
		"worker_id", workerID,
		"poll_interval", 500*time.Millisecond,
	)

	recoveryWorkerID := fmt.Sprintf(
		"%s-recovery-%d",
		hostname,
		os.Getpid(),
	)

	recoveryProcessor, err := execution.NewRecoveryProcessor(
		pool,
		map[string]execution.Provider{
			appJobProvider: developmentProvider,
		},
		recoveryWorkerID,
		30*time.Second,
		execution.MaxRecoveryAttempts,
	)
	if err != nil {
		slog.Error("failed to create recovery processor", "error", err)
		os.Exit(1)
	}

	slog.Info(
		"recovery processor started",
		"worker_id", recoveryWorkerID,
		"poll_interval", 5*time.Second,
	)

	// registry := prometheus.NewRegistry()

	// metrics, err := observability.NewMetrics(registry)
	// if err != nil {
	// 	slog.Error("failed to initialize Prometheus metrics", "error", err)
	// 	os.Exit(1)
	// }

	w := &worker.Worker{
		WorkerID:             workerID,
		Pool:                 pool,
		Handler:              appHandler,
		OperationExecutor:    operationService,
		PollInterval:         500 * time.Millisecond,
		Metrics:              metrics,
		ShutdownDrainTimeout: 25 * time.Second,
	}

	// The signal context controls the application's shutdown sequence.
	signalCtx, stopSignals := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stopSignals()

	// The worker context is cancelled only after HTTP has drained.
	workerCtx, workerCancel := context.WithCancel(
		context.Background(),
	)
	defer workerCancel()

	var backgroundWG sync.WaitGroup
	backgroundErrCh := make(chan error, 2)

	// Pending-job metrics loop.
	backgroundWG.Add(1)
	go func() {
		defer backgroundWG.Done()

		runPendingJobsMetrics(
			workerCtx,
			pool,
			metrics,
			15*time.Second,
		)
	}()

	// Main worker.
	backgroundWG.Add(1)
	go func() {
		defer backgroundWG.Done()

		if err := w.Run(workerCtx); err != nil &&
			workerCtx.Err() == nil {
			backgroundErrCh <- fmt.Errorf(
				"worker stopped unexpectedly: %w",
				err,
			)
		}
	}()

	// Recovery processor.
	backgroundWG.Add(1)
	go func() {
		defer backgroundWG.Done()

		if err := recoveryProcessor.Run(
			workerCtx,
			5*time.Second,
		); err != nil && workerCtx.Err() == nil {
			backgroundErrCh <- fmt.Errorf(
				"recovery processor stopped unexpectedly: %w",
				err,
			)
		}
	}()

	metricsHandler, err := observability.NewMetricsHandler(registry)
	if err != nil {
		slog.Error("failed to create metrics handler", "error", err)
		os.Exit(1)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/ready", readinessHandler(pool))
	mux.HandleFunc("/jobs", createJobHandler(pool, metrics))
	mux.HandleFunc("/jobs/{id}", getJobHandler(pool))
	mux.HandleFunc(
		"PATCH /jobs/{id}/status",
		transitionJobStatusHandler(pool),
	)

	mux.Handle("GET /metrics", metricsHandler)

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

	server := &http.Server{
		Addr:    ":8081",
		Handler: mux,
	}

	serverErrCh := make(chan error, 1)

	slog.Info(
		"HTTP server starting",
		"address", server.Addr,
	)

	backgroundWG.Add(1)
	go func() {
		defer backgroundWG.Done()

		if err := server.ListenAndServe(); err != nil &&
			!errors.Is(err, http.ErrServerClosed) {
			serverErrCh <- err
		}
	}()

	// Wait for a shutdown signal or an unexpected component failure.
	select {
	case <-signalCtx.Done():
		slog.Info("shutdown signal received")

	case err := <-serverErrCh:
		slog.Error("HTTP server failed", "error", err)

	case err := <-backgroundErrCh:
		slog.Error("background component failed", "error", err)
	}

	// Step 1: Stop accepting HTTP requests and drain active requests.
	httpShutdownCtx, httpShutdownCancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)

	if err := server.Shutdown(httpShutdownCtx); err != nil {
		slog.Error(
			"HTTP server shutdown failed",
			"error", err,
		)
		_ = server.Close()
	}

	httpShutdownCancel()

	// Step 2: Stop new worker claims and recovery/metrics loops.
	// Already-claimed jobs receive their configured drain period.
	slog.Info(
		"stopping background workers",
		"job_drain_timeout", w.ShutdownDrainTimeout,
	)
	workerCancel()

	// Step 3: Wait for all background goroutines before closing the DB.
	backgroundDone := make(chan struct{})

	go func() {
		backgroundWG.Wait()
		close(backgroundDone)
	}()

	shutdownCtx, shutdownCancel := context.WithTimeout(
		context.Background(),
		35*time.Second,
	)
	defer shutdownCancel()

	select {
	case <-backgroundDone:
		slog.Info("all background goroutines stopped")

	case <-shutdownCtx.Done():
		slog.Error(
			"background shutdown exceeded its deadline; waiting before closing database",
			"error", shutdownCtx.Err(),
		)

		// Keep the database pool open until its users have stopped.
		// Docker's stop_grace_period provides the outer process limit.
		<-backgroundDone
	}

	slog.Info("Chronos shutdown complete")
}
