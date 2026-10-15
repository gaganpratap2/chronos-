
package database

import (
    "context"
    "testing"
    "time"

    "chronos/job"
    "github.com/joho/godotenv"
)


func TestSaveAndGetJob(t *testing.T) {

	// if err := godotenv.Load(); err != nil {
    // 	t.Log(".env not found; using system environment variables")
	// }


	if err := godotenv.Load("../.env"); err != nil {
    	t.Log(".env not found; using system environment variables")
	}


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

    original.RetryCount = 2
    original.MaxRetries = 5

	if err := SaveJob(ctx, pool, original); err != nil {
		t.Fatalf("SaveJob failed: %v", err)
	}

	retrieved, err := GetJob(ctx, pool, testID)
	if err != nil {
		t.Fatalf("GetJob failed: %v", err)
	}

    if original.RetryCount != 2 {
        t.Errorf("expected RetryCount 2, got %d", original.RetryCount)
    }

    if original.MaxRetries != 5 {
        t.Errorf("expected MaxRetries 5, got %d", original.MaxRetries)
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




func TestRetryIncrementsCount(t *testing.T) {
    j, err := job.NewJob(201, "payment")
    if err != nil {
        t.Fatal(err)
    }

    if err := j.TransitionTo("Running"); err != nil {
        t.Fatal(err)
    }
    if err := j.TransitionTo("Failed"); err != nil {
        t.Fatal(err)
    }

    if err := j.Retry(); err != nil {
        t.Fatalf("expected retry to succeed: %v", err)
    }

    if j.RetryCount != 1 {
        t.Errorf("expected RetryCount 1, got %d", j.RetryCount)
    }
    if j.Status != "Pending" {
        t.Errorf("expected Pending, got %s", j.Status)
    }
}

func TestRetryRejectsNonFailedJob(t *testing.T) {
    j, err := job.NewJob(202, "email")
    if err != nil {
        t.Fatal(err)
    }

    err = j.Retry()

    if err == nil {
        t.Fatal("expected retry to fail for a Pending job")
    }

    if j.RetryCount != 0 {
        t.Errorf("expected RetryCount 0, got %d", j.RetryCount)
    }
}

func TestRetryLimit(t *testing.T) {
    j, err := job.NewJob(203, "report")
    if err != nil {
        t.Fatal(err)
    }

    for i := 0; i < j.MaxRetries; i++ {
        if err := j.TransitionTo("Running"); err != nil {
            t.Fatal(err)
        }
        if err := j.TransitionTo("Failed"); err != nil {
            t.Fatal(err)
        }
        if err := j.Retry(); err != nil {
            t.Fatalf("retry %d should succeed: %v", i+1, err)
        }
    }

    if j.RetryCount != j.MaxRetries {
        t.Fatalf("expected %d retries, got %d",
            j.MaxRetries, j.RetryCount)
    }

    // Fail once more after the retry limit has been reached.
    if err := j.TransitionTo("Running"); err != nil {
        t.Fatal(err)
    }
    if err := j.TransitionTo("Failed"); err != nil {
        t.Fatal(err)
    }

    if err := j.Retry(); err == nil {
        t.Fatal("expected retry to fail after reaching the limit")
    }

    if j.RetryCount != j.MaxRetries {
        t.Errorf("retry count should remain %d, got %d",
            j.MaxRetries, j.RetryCount)
    }
    if j.Status != "Failed" {
        t.Errorf("expected Failed after exhausted retries, got %s",
            j.Status)
    }
}
