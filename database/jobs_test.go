
package database

import (
	"context"
	"testing"
	"time"

	"chronos/job"
)

func TestSaveAndGetJob(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()

	// Use a unique ID so we don't overwrite existing data.
	const testID = 987654

	_, err = pool.Exec(ctx, "DELETE FROM jobs WHERE id = $1", testID)
	if err != nil {
		t.Fatalf("failed to prepare test: %v", err)
	}
	defer pool.Exec(context.Background(),
		"DELETE FROM jobs WHERE id = $1", testID)

	original, err := job.NewJob(testID, "automated-test")
	if err != nil {
		t.Fatal(err)
	}

	if err := SaveJob(ctx, pool, original); err != nil {
		t.Fatalf("SaveJob failed: %v", err)
	}

	retrieved, err := GetJob(ctx, pool, testID)
	if err != nil {
		t.Fatalf("GetJob failed: %v", err)
	}

	if retrieved.ID != original.ID {
		t.Errorf("expected ID %d, got %d", original.ID, retrieved.ID)
	}

	if retrieved.Name != original.Name {
		t.Errorf("expected name %q, got %q", original.Name, retrieved.Name)
	}

	if retrieved.Status != original.Status {
		t.Errorf("expected status %q, got %q",
			original.Status, retrieved.Status)
	}
}
