
package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"chronos/database"
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
		context.Background(), 10*time.Second,
	)
	defer cancel()

	pool, err := database.Connect(ctx)
	if err != nil {
		t.Fatalf("database connection failed: %v", err)
	}
	defer pool.Close()

	const testID = 987655

	_, err = pool.Exec(ctx, "DELETE FROM jobs WHERE id = $1", testID)
	if err != nil {
		t.Fatalf("test setup failed: %v", err)
	}
	defer pool.Exec(context.Background(),
		"DELETE FROM jobs WHERE id = $1", testID)

	mux := http.NewServeMux()
	mux.HandleFunc("/jobs", createJobHandler(pool))
	mux.HandleFunc("/jobs/{id}", getJobHandler(pool))

	// 1. Create a job.
	createReq := httptest.NewRequest(
		http.MethodPost,
		"/jobs",
		strings.NewReader(`{"id":987655,"name":"api-test"}`),
	)
	createReq.Header.Set("Content-Type", "application/json")

	createRec := httptest.NewRecorder()
	mux.ServeHTTP(createRec, createReq)

	if createRec.Code != http.StatusCreated {
		t.Fatalf("create: expected 201, got %d: %s",
			createRec.Code, createRec.Body.String())
	}

	// 2. Try creating the same ID again.
	duplicateReq := httptest.NewRequest(
		http.MethodPost,
		"/jobs",
		strings.NewReader(`{"id":987655,"name":"api-test"}`),
	)
	duplicateReq.Header.Set("Content-Type", "application/json")

	duplicateRec := httptest.NewRecorder()
	mux.ServeHTTP(duplicateRec, duplicateReq)

	if duplicateRec.Code != http.StatusConflict {
		t.Errorf("duplicate: expected 409, got %d",
			duplicateRec.Code)
	}

	// 3. Retrieve the created job.
	getReq := httptest.NewRequest(
		http.MethodGet, "/jobs/987655", nil,
	)
	getRec := httptest.NewRecorder()
	mux.ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf("get: expected 200, got %d: %s",
			getRec.Code, getRec.Body.String())
	}

	if !strings.Contains(getRec.Body.String(), `"Name":"api-test"`) {
		t.Errorf("unexpected job response: %s", getRec.Body.String())
	}

	// 4. Send invalid JSON.
	invalidReq := httptest.NewRequest(
		http.MethodPost,
		"/jobs",
		strings.NewReader(`{invalid-json}`),
	)
	invalidReq.Header.Set("Content-Type", "application/json")

	invalidRec := httptest.NewRecorder()
	mux.ServeHTTP(invalidRec, invalidReq)

	if invalidRec.Code != http.StatusBadRequest {
		t.Errorf("invalid JSON: expected 400, got %d",
			invalidRec.Code)
	}

	// 5. Request a missing job.
	missingReq := httptest.NewRequest(
		http.MethodGet, "/jobs/999999", nil,
	)
	missingRec := httptest.NewRecorder()
	mux.ServeHTTP(missingRec, missingReq)

	if missingRec.Code != http.StatusNotFound {
		t.Errorf("missing job: expected 404, got %d",
			missingRec.Code)
	}
}


func TestTransitionJobAPI(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(), 10*time.Second,
	)
	defer cancel()

	pool, err := database.Connect(ctx)
	if err != nil {
		t.Fatalf("database connection failed: %v", err)
	}
	defer pool.Close()

	const testID = 987656

	_, err = pool.Exec(ctx, "DELETE FROM jobs WHERE id = $1", testID)
	if err != nil {
		t.Fatalf("test setup failed: %v", err)
	}
	defer pool.Exec(context.Background(),
		"DELETE FROM jobs WHERE id = $1", testID)

	mux := http.NewServeMux()
	mux.HandleFunc("/jobs", createJobHandler(pool))
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
