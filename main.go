package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
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
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var request CreateJobRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}

		newJob, err := job.NewJob(request.ID, request.Name)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if err := database.SaveJob(r.Context(), pool, newJob); err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" {
				http.Error(w, "job ID already exists", http.StatusConflict)
				return
			}

			log.Printf("Failed to save job %d: %v", newJob.ID, err)
			http.Error(w, "failed to save job", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)

		if err := json.NewEncoder(w).Encode(newJob); err != nil {
			log.Printf("Response encoding error: %v", err)
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

type AppHandler struct{}

func (h AppHandler) Execute(ctx context.Context, j job.Job) error {
	log.Printf("Executing job %d: %s", j.ID, j.Name)
	return nil
}

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

	workerID := fmt.Sprintf("%s-%d", hostname, os.Getpid())
	log.Printf("Starting worker with ID: %s", workerID)

	w := &worker.Worker{
		WorkerID:     workerID,
		Pool:         pool,
		Handler:      appHandler,
		PollInterval: 500 * time.Millisecond,
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
