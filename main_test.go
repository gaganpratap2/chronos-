package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"chronos/database"
	"chronos/observability"
	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus"
)

func TestHealthHandler(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	healthHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	if !strings.Contains(rec.Body.String(), `"status":"ok"`) {
		t.Errorf("unexpected health response: %s", rec.Body.String())
	}
}

func TestJobAPI(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		15*time.Second,
	)
	defer cancel()

	if err := godotenv.Load(); err != nil {
		t.Log(".env not found; using system environment variables")
	}

	pool, err := database.Connect(ctx)
	if err != nil {
		t.Fatalf("database connection failed: %v", err)
	}
	defer pool.Close()

	const testID = 987655
	const testKey = "api-test-key-987655"
	const differentKey = "api-test-key-987655-different"

	// Clean up previous idempotency records first.
	_, err = pool.Exec(
		ctx,
		`DELETE FROM idempotency_records
		 WHERE operation_scope = $1
		   AND idempotency_key IN ($2, $3)`,
		createJobOperationScope,
		testKey,
		differentKey,
	)
	if err != nil {
		t.Fatalf("idempotency test cleanup failed: %v", err)
	}

	// Clean up the previous job.
	_, err = pool.Exec(
		ctx,
		"DELETE FROM jobs WHERE id = $1",
		testID,
	)
	if err != nil {
		t.Fatalf("test setup failed: %v", err)
	}

	// Cleanup after the test.
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cleanupCancel()

		// Delete idempotency records before deleting the job.
		_, err := pool.Exec(
			cleanupCtx,
			`DELETE FROM idempotency_records
			 WHERE operation_scope = $1
			   AND idempotency_key IN ($2, $3)`,
			createJobOperationScope,
			testKey,
			differentKey,
		)
		if err != nil {
			t.Errorf("idempotency cleanup failed: %v", err)
		}

		_, err = pool.Exec(
			cleanupCtx,
			"DELETE FROM jobs WHERE id = $1",
			testID,
		)
		if err != nil {
			t.Errorf("job cleanup failed: %v", err)
		}
	}()

	// mux := http.NewServeMux()
	// mux.HandleFunc("/jobs", createJobHandler(pool))

	registry := prometheus.NewRegistry()
	metrics, err := observability.NewMetrics(registry)
	if err != nil {
		t.Fatalf("failed to initialize metrics: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/jobs", createJobHandler(pool, metrics))
	mux.HandleFunc("/jobs/{id}", getJobHandler(pool))

	// 1. Create a new job.
	createReq := httptest.NewRequest(
		http.MethodPost,
		"/jobs",
		strings.NewReader(
			`{"id":987655,"name":"api-test"}`,
		),
	)
	createReq.Header.Set("Content-Type", "application/json")
	createReq.Header.Set("Idempotency-Key", testKey)

	createRec := httptest.NewRecorder()
	mux.ServeHTTP(createRec, createReq)

	if createRec.Code != http.StatusCreated {
		t.Fatalf(
			"create: expected 201, got %d: %s",
			createRec.Code,
			createRec.Body.String(),
		)
	}

	// 2. Retry the same request using the same key.
	// Chronos must replay the original response.
	duplicateReq := httptest.NewRequest(
		http.MethodPost,
		"/jobs",
		strings.NewReader(
			`{"id":987655,"name":"api-test"}`,
		),
	)
	duplicateReq.Header.Set("Content-Type", "application/json")
	duplicateReq.Header.Set("Idempotency-Key", testKey)

	duplicateRec := httptest.NewRecorder()
	mux.ServeHTTP(duplicateRec, duplicateReq)

	if duplicateRec.Code != http.StatusCreated {
		t.Fatalf(
			"duplicate: expected replay status 201, got %d: %s",
			duplicateRec.Code,
			duplicateRec.Body.String(),
		)
	}

	if duplicateRec.Body.String() != createRec.Body.String() {
		t.Errorf(
			"expected original response %q, got %q",
			createRec.Body.String(),
			duplicateRec.Body.String(),
		)
	}

	// 3. Reuse the same key with a different payload.
	// Chronos must reject it.
	conflictReq := httptest.NewRequest(
		http.MethodPost,
		"/jobs",
		strings.NewReader(
			`{"id":987655,"name":"different-job"}`,
		),
	)
	conflictReq.Header.Set("Content-Type", "application/json")
	conflictReq.Header.Set("Idempotency-Key", testKey)

	conflictRec := httptest.NewRecorder()
	mux.ServeHTTP(conflictRec, conflictReq)

	if conflictRec.Code != http.StatusConflict {
		t.Errorf(
			"changed payload: expected 409, got %d: %s",
			conflictRec.Code,
			conflictRec.Body.String(),
		)
	}

	// 4. Use a new key but an already existing job ID.
	// PostgreSQL must reject the duplicate job ID.
	duplicateIDReq := httptest.NewRequest(
		http.MethodPost,
		"/jobs",
		strings.NewReader(
			`{"id":987655,"name":"api-test"}`,
		),
	)
	duplicateIDReq.Header.Set(
		"Content-Type",
		"application/json",
	)
	duplicateIDReq.Header.Set(
		"Idempotency-Key",
		differentKey,
	)

	duplicateIDRec := httptest.NewRecorder()
	mux.ServeHTTP(duplicateIDRec, duplicateIDReq)

	if duplicateIDRec.Code != http.StatusConflict {
		t.Errorf(
			"duplicate job ID: expected 409, got %d: %s",
			duplicateIDRec.Code,
			duplicateIDRec.Body.String(),
		)
	}

	// 5. Retrieve the created job.
	getReq := httptest.NewRequest(
		http.MethodGet,
		"/jobs/987655",
		nil,
	)

	getRec := httptest.NewRecorder()
	mux.ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf(
			"get: expected 200, got %d: %s",
			getRec.Code,
			getRec.Body.String(),
		)
	}

	if !strings.Contains(
		getRec.Body.String(),
		`"Name":"api-test"`,
	) {
		t.Errorf(
			"unexpected job response: %s",
			getRec.Body.String(),
		)
	}

	// 6. Send invalid JSON with an idempotency key.
	invalidReq := httptest.NewRequest(
		http.MethodPost,
		"/jobs",
		strings.NewReader(`{invalid-json}`),
	)
	invalidReq.Header.Set("Content-Type", "application/json")
	invalidReq.Header.Set(
		"Idempotency-Key",
		"invalid-json-test-key",
	)

	invalidRec := httptest.NewRecorder()
	mux.ServeHTTP(invalidRec, invalidReq)

	if invalidRec.Code != http.StatusBadRequest {
		t.Errorf(
			"invalid JSON: expected 400, got %d",
			invalidRec.Code,
		)
	}

	// 7. Send a valid request without an idempotency key.
	missingKeyReq := httptest.NewRequest(
		http.MethodPost,
		"/jobs",
		strings.NewReader(
			`{"id":987699,"name":"missing-key-test"}`,
		),
	)
	missingKeyReq.Header.Set(
		"Content-Type",
		"application/json",
	)

	missingKeyRec := httptest.NewRecorder()
	mux.ServeHTTP(missingKeyRec, missingKeyReq)

	if missingKeyRec.Code != http.StatusBadRequest {
		t.Errorf(
			"missing key: expected 400, got %d",
			missingKeyRec.Code,
		)
	}

	// 8. Request a job that does not exist.
	missingReq := httptest.NewRequest(
		http.MethodGet,
		"/jobs/999999",
		nil,
	)

	missingRec := httptest.NewRecorder()
	mux.ServeHTTP(missingRec, missingReq)

	if missingRec.Code != http.StatusNotFound {
		t.Errorf(
			"missing job: expected 404, got %d",
			missingRec.Code,
		)
	}
}

func TestTransitionJobAPI(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(), 10*time.Second,
	)
	defer cancel()

	if err := godotenv.Load(); err != nil {
		t.Log(".env not found; using system environment variables")
	}

	pool, err := database.Connect(ctx)
	if err != nil {
		t.Fatalf("database connection failed: %v", err)
	}
	defer pool.Close()

	const testID = 987656
	const testKey = "transition-test-key-987656"

	_, err = pool.Exec(ctx, "DELETE FROM jobs WHERE id = $1", testID)
	if err != nil {
		t.Fatalf("test setup failed: %v", err)
	}

	_, err = pool.Exec(
		ctx,
		`DELETE FROM idempotency_records
     WHERE operation_scope = $1
       AND idempotency_key = $2`,
		createJobOperationScope,
		testKey,
	)

	if err != nil {
		t.Fatalf("idempotency cleanup failed: %v", err)
	}

	_, err = pool.Exec(
		ctx,
		"DELETE FROM jobs WHERE id = $1",
		testID,
	)

	if err != nil {
		t.Fatalf("test setup failed: %v", err)
	}

	// defer pool.Exec(context.Background(),
	// 	"DELETE FROM jobs WHERE id = $1", testID)

	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cleanupCancel()

		// Delete the referencing idempotency record first.
		_, err := pool.Exec(
			cleanupCtx,
			`DELETE FROM idempotency_records
         WHERE operation_scope = $1
           AND idempotency_key = $2`,
			createJobOperationScope,
			testKey,
		)
		if err != nil {
			t.Errorf("idempotency cleanup failed: %v", err)
		}

		// Now delete the job.
		_, err = pool.Exec(
			cleanupCtx,
			"DELETE FROM jobs WHERE id = $1",
			testID,
		)
		if err != nil {
			t.Errorf("job cleanup failed: %v", err)
		}
	}()

	registry := prometheus.NewRegistry()
	metrics, err := observability.NewMetrics(registry)
	if err != nil {
		t.Fatalf("failed to initialize metrics: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/jobs", createJobHandler(pool, metrics))
	mux.HandleFunc("/jobs/{id}", getJobHandler(pool))
	mux.HandleFunc(
		"PATCH /jobs/{id}/status",
		transitionJobStatusHandler(pool),
	)

	// Create a new job.
	createReq := httptest.NewRequest(
		http.MethodPost,
		"/jobs",
		strings.NewReader(
			`{"id":987656,"name":"transition-test"}`,
		),
	)
	createReq.Header.Set("Content-Type", "application/json")
	createReq.Header.Set("Idempotency-Key", testKey)

	createRec := httptest.NewRecorder()
	mux.ServeHTTP(createRec, createReq)

	if createRec.Code != http.StatusCreated {
		t.Fatalf("create: expected 201, got %d: %s",
			createRec.Code, createRec.Body.String())
	}

	// Test valid and invalid transitions.
	transitions := []struct {
		status       string
		expectedCode int
	}{
		{"Running", http.StatusOK},
		{"Completed", http.StatusOK},
		{"Pending", http.StatusBadRequest},
	}

	for _, transition := range transitions {
		body := `{"status":"` + transition.status + `"}`

		req := httptest.NewRequest(
			http.MethodPatch,
			"/jobs/987656/status",
			strings.NewReader(body),
		)
		req.Header.Set("Content-Type", "application/json")

		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)

		if rec.Code != transition.expectedCode {
			t.Errorf(
				"transition to %s: expected %d, got %d: %s",
				transition.status,
				transition.expectedCode,
				rec.Code,
				rec.Body.String(),
			)
		}
	}

	// Verify the rejected transition did not change the database.
	getReq := httptest.NewRequest(
		http.MethodGet, "/jobs/987656", nil,
	)
	getRec := httptest.NewRecorder()
	mux.ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf("get: expected 200, got %d", getRec.Code)
	}

	if !strings.Contains(getRec.Body.String(), `"Status":"Completed"`) {
		t.Errorf("expected persisted status Completed, got: %s",
			getRec.Body.String())
	}
}

// package main

// import (
// 	"encoding/json"
// 	"net/http"
// 	"net/http/httptest"
// 	"strings"
// 	"testing"

// 	"chronos/job"
// )

// func resetJobs() {
// 	jobsMu.Lock()
// 	defer jobsMu.Unlock()
// 	jobs = make(map[int]job.Job)
// }

// func TestHealthHandler(t *testing.T) {
// 	req := httptest.NewRequest(http.MethodGet, "/health", nil)
// 	rec := httptest.NewRecorder()

// 	healthHandler(rec, req)

// 	if rec.Code != http.StatusOK {
// 		t.Fatalf("expected status 200, got %d", rec.Code)
// 	}
// }

// func TestCreateJobHandler(t *testing.T) {
// 	resetJobs()

// 	body := `{"id":101,"name":"payment"}`
// 	req := httptest.NewRequest(
// 		http.MethodPost,
// 		"/jobs",
// 		strings.NewReader(body),
// 	)
// 	req.Header.Set("Content-Type", "application/json")

// 	rec := httptest.NewRecorder()
// 	createJobHandler(rec, req)

// 	if rec.Code != http.StatusCreated {
// 		t.Fatalf("expected status 201, got %d: %s",
// 			rec.Code, rec.Body.String())
// 	}

// 	var created job.Job
// 	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
// 		t.Fatalf("invalid JSON response: %v", err)
// 	}

// 	if created.ID != 101 {
// 		t.Errorf("expected ID 101, got %d", created.ID)
// 	}

// 	if created.Name != "payment" {
// 		t.Errorf("expected name payment, got %s", created.Name)
// 	}

// 	if created.Status != "Pending" {
// 		t.Errorf("expected status Pending, got %s", created.Status)
// 	}
// }

// func TestDuplicateJobID(t *testing.T) {
// 	resetJobs()

// 	create := func() int {
// 		body := `{"id":101,"name":"payment"}`
// 		req := httptest.NewRequest(
// 			http.MethodPost,
// 			"/jobs",
// 			strings.NewReader(body),
// 		)
// 		rec := httptest.NewRecorder()
// 		createJobHandler(rec, req)
// 		return rec.Code
// 	}

// 	if status := create(); status != http.StatusCreated {
// 		t.Fatalf("first request: expected 201, got %d", status)
// 	}

// 	if status := create(); status != http.StatusConflict {
// 		t.Errorf("duplicate request: expected 409, got %d", status)
// 	}
// }

func TestReadinessHandlerRejectsNonGetMethod(t *testing.T) {
	handler := readinessHandler(nil)

	req := httptest.NewRequest(http.MethodPost, "/ready", nil)
	rec := httptest.NewRecorder()

	handler(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected status 405, got %d", rec.Code)
	}
}

func TestReadinessHandlerRejectsNilPool(t *testing.T) {
	handler := readinessHandler(nil)

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	rec := httptest.NewRecorder()

	handler(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status 503, got %d", rec.Code)
	}

	if !strings.Contains(rec.Body.String(), `"status":"not_ready"`) {
		t.Errorf("unexpected readiness response: %s", rec.Body.String())
	}
}
