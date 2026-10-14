
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"chronos/job"
)

type CreateJobRequest struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

var (
	jobs   = make(map[int]job.Job)
	jobsMu sync.Mutex
)

func healthHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, `{"status":"ok"}`)
}

func createJobHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}	

	var request CreateJobRequest

	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	newJob, err := job.NewJob(request.ID, request.Name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	jobsMu.Lock()
	defer jobsMu.Unlock()

	if _, exists := jobs[newJob.ID]; exists {
		http.Error(w, "job ID already exists", http.StatusConflict)
		return
	}	

	jobs[newJob.ID] = newJob

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)

	if err := json.NewEncoder(w).Encode(newJob); err != nil {
		fmt.Println("Response encoding error:", err)
	}
}

func main() {
	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/jobs", createJobHandler)

	fmt.Println("Chronos API listening on http://localhost:8080")
	
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		fmt.Println("Server error:", err)
	}
}
