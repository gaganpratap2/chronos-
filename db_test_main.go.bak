
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"chronos/database"
	"chronos/job"
)

func main() {
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

	fmt.Println("Connected to PostgreSQL!")

	// Create a job in Go.
	j := job.Job{
		ID:     202,
		Name:   "email",
		Status: "Pending",
	}

	// Save the job.
	if err := database.SaveJob(ctx, pool, j); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Job saved successfully!")

	// Retrieve the job from PostgreSQL.
	savedJob, err := database.GetJob(ctx, pool, 202)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Retrieved job: %+v\n", savedJob)
}
