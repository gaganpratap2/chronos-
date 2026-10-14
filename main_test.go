
package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"chronos/job"
)

func resetJobs() {
	jobsMu.Lock()
	defer jobsMu.Unlock()
	jobs = make(map[int]job.Job)
}

func TestHealthHandler(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	healthHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
}

func TestCreateJobHandler(t *testing.T) {
	resetJobs()

	body := `{"id":101,"name":"payment"}`
	req := httptest.NewRequest(
		http.MethodPost,
		"/jobs",
		strings.NewReader(body),
	)
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	createJobHandler(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected status 201, got %d: %s",
			rec.Code, rec.Body.String())
	}

	var created job.Job
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("invalid JSON response: %v", err)
	}

	if created.ID != 101 {
		t.Errorf("expected ID 101, got %d", created.ID)
	}

	if created.Name != "payment" {
		t.Errorf("expected name payment, got %s", created.Name)
	}

	if created.Status != "Pending" {
		t.Errorf("expected status Pending, got %s", created.Status)
	}
}

func TestDuplicateJobID(t *testing.T) {
	resetJobs()

	create := func() int {
		body := `{"id":101,"name":"payment"}`
		req := httptest.NewRequest(
			http.MethodPost,
			"/jobs",
			strings.NewReader(body),
		)
		rec := httptest.NewRecorder()
		createJobHandler(rec, req)
		return rec.Code
	}

	if status := create(); status != http.StatusCreated {
		t.Fatalf("first request: expected 201, got %d", status)
	}

	if status := create(); status != http.StatusConflict {
		t.Errorf("duplicate request: expected 409, got %d", status)
	}
}
