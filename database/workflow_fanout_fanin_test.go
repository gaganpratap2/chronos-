package database

import (
	"context"
	"testing"

	"github.com/joho/godotenv"
)

func TestWorkflowFanOutFanIn(t *testing.T) {
	if err := godotenv.Load("../.env"); err != nil {
		t.Fatalf("load .env: %v", err)
	}

	ctx := context.Background()

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("connect database: %v", err)
	}
	defer pool.Close()

	const (
		workflowID int64 = 999910

		taskA int64 = 999911
		taskB int64 = 999912
		taskC int64 = 999913
		taskD int64 = 999914
	)

	// Clean previous test data.
	_, err = pool.Exec(
		ctx,
		`DELETE FROM workflows WHERE id = $1`,
		workflowID,
	)
	if err != nil {
		t.Fatalf("cleanup workflow: %v", err)
	}

	// Create workflow.
	_, err = pool.Exec(
		ctx,
		`
		INSERT INTO workflows (
			id,
			name,
			status
		)
		VALUES ($1, 'fan-out-fan-in-test', 'Pending')
		`,
		workflowID,
	)
	if err != nil {
		t.Fatalf("insert workflow: %v", err)
	}

	// Create tasks.
	tasks := []struct {
		id   int64
		name string
	}{
		{taskA, "A"},
		{taskB, "B"},
		{taskC, "C"},
		{taskD, "D"},
	}

	for _, task := range tasks {
		_, err = pool.Exec(
			ctx,
			`
			INSERT INTO workflow_tasks (
				id,
				workflow_id,
				name,
				status
			)
			VALUES ($1, $2, $3, 'Pending')
			`,
			task.id,
			workflowID,
			task.name,
		)
		if err != nil {
			t.Fatalf(
				"insert task %s: %v",
				task.name,
				err,
			)
		}
	}

	// A → B
	_, err = pool.Exec(
		ctx,
		`
		INSERT INTO task_dependencies (
			task_id,
			depends_on_task_id
		)
		VALUES ($1, $2)
		`,
		taskB,
		taskA,
	)
	if err != nil {
		t.Fatalf("insert A -> B dependency: %v", err)
	}

	// A → C
	_, err = pool.Exec(
		ctx,
		`
		INSERT INTO task_dependencies (
			task_id,
			depends_on_task_id
		)
		VALUES ($1, $2)
		`,
		taskC,
		taskA,
	)
	if err != nil {
		t.Fatalf("insert A -> C dependency: %v", err)
	}

	// B → D
	_, err = pool.Exec(
		ctx,
		`
		INSERT INTO task_dependencies (
			task_id,
			depends_on_task_id
		)
		VALUES ($1, $2)
		`,
		taskD,
		taskB,
	)
	if err != nil {
		t.Fatalf("insert B -> D dependency: %v", err)
	}

	// C → D
	_, err = pool.Exec(
		ctx,
		`
		INSERT INTO task_dependencies (
			task_id,
			depends_on_task_id
		)
		VALUES ($1, $2)
		`,
		taskD,
		taskC,
	)
	if err != nil {
		t.Fatalf("insert C -> D dependency: %v", err)
	}

	defer pool.Exec(
		context.Background(),
		`DELETE FROM workflows WHERE id = $1`,
		workflowID,
	)

	// --------------------------------------------------
	// STEP 1: Only A should be runnable.
	// --------------------------------------------------

	dispatched, err := ClaimTaskAndCreateJob(ctx, pool)
	if err != nil {
		t.Fatalf("claim A: %v", err)
	}

	if dispatched == nil {
		t.Fatal("expected A to be dispatched")
	}

	if dispatched.TaskID != taskA {
		t.Fatalf(
			"expected task A (%d), got %d",
			taskA,
			dispatched.TaskID,
		)
	}

	// Mark A completed.
	_, err = pool.Exec(
		ctx,
		`
		UPDATE workflow_tasks
		SET status = 'Completed',
		    updated_at = NOW()
		WHERE id = $1
		`,
		taskA,
	)
	if err != nil {
		t.Fatalf("complete A: %v", err)
	}

	// --------------------------------------------------
	// STEP 2: B and C should now be runnable.
	// --------------------------------------------------

	dispatchedB, err := ClaimTaskAndCreateJob(ctx, pool)
	if err != nil {
		t.Fatalf("claim B/C first: %v", err)
	}

	if dispatchedB == nil {
		t.Fatal("expected B or C to be dispatched")
	}

	dispatchedC, err := ClaimTaskAndCreateJob(ctx, pool)
	if err != nil {
		t.Fatalf("claim B/C second: %v", err)
	}

	if dispatchedC == nil {
		t.Fatal("expected second branch task to be dispatched")
	}

	if dispatchedB.TaskID == dispatchedC.TaskID {
		t.Fatalf(
			"expected different branch tasks, both were %d",
			dispatchedB.TaskID,
		)
	}

	// Verify B and C were the two dispatched tasks.
	branchTasks := map[int64]bool{
		dispatchedB.TaskID: true,
		dispatchedC.TaskID: true,
	}

	if !branchTasks[taskB] || !branchTasks[taskC] {
		t.Fatalf(
			"expected B and C to be dispatched, got %d and %d",
			dispatchedB.TaskID,
			dispatchedC.TaskID,
		)
	}

	// --------------------------------------------------
	// STEP 3: D must NOT be runnable yet.
	// --------------------------------------------------

	notReady, err := ClaimTaskAndCreateJob(ctx, pool)
	if err != nil {
		t.Fatalf("check D before branches complete: %v", err)
	}

	if notReady != nil {
		t.Fatalf(
			"D was dispatched before both dependencies completed: task=%d",
			notReady.TaskID,
		)
	}

	// --------------------------------------------------
	// STEP 4: Complete B only.
	// D must STILL remain blocked.
	// --------------------------------------------------

	_, err = pool.Exec(
		ctx,
		`
		UPDATE workflow_tasks
		SET status = 'Completed',
		    updated_at = NOW()
		WHERE id = $1
		`,
		taskB,
	)
	if err != nil {
		t.Fatalf("complete B: %v", err)
	}

	notReady, err = ClaimTaskAndCreateJob(ctx, pool)
	if err != nil {
		t.Fatalf("check D after only B: %v", err)
	}

	if notReady != nil {
		t.Fatalf(
			"D was dispatched after only one dependency completed: task=%d",
			notReady.TaskID,
		)
	}

	// --------------------------------------------------
	// STEP 5: Complete C.
	// Now D should become runnable.
	// --------------------------------------------------

	_, err = pool.Exec(
		ctx,
		`
		UPDATE workflow_tasks
		SET status = 'Completed',
		    updated_at = NOW()
		WHERE id = $1
		`,
		taskC,
	)
	if err != nil {
		t.Fatalf("complete C: %v", err)
	}

	dispatchedD, err := ClaimTaskAndCreateJob(ctx, pool)
	if err != nil {
		t.Fatalf("claim D: %v", err)
	}

	if dispatchedD == nil {
		t.Fatal("expected D to be dispatched")
	}

	if dispatchedD.TaskID != taskD {
		t.Fatalf(
			"expected D (%d), got %d",
			taskD,
			dispatchedD.TaskID,
		)
	}

	// --------------------------------------------------
	// STEP 6: Complete D.
	// --------------------------------------------------

	_, err = pool.Exec(
		ctx,
		`
		UPDATE workflow_tasks
		SET status = 'Completed',
		    updated_at = NOW()
		WHERE id = $1
		`,
		taskD,
	)
	if err != nil {
		t.Fatalf("complete D: %v", err)
	}

	// --------------------------------------------------
	// STEP 7: Workflow should become Completed.
	// --------------------------------------------------

	if err := UpdateWorkflowStatus(
		ctx,
		pool,
		workflowID,
	); err != nil {
		t.Fatalf("update workflow status: %v", err)
	}

	var workflowStatus string

	err = pool.QueryRow(
		ctx,
		`
		SELECT status
		FROM workflows
		WHERE id = $1
		`,
		workflowID,
	).Scan(&workflowStatus)
	if err != nil {
		t.Fatalf("read workflow status: %v", err)
	}

	if workflowStatus != "Completed" {
		t.Fatalf(
			"expected workflow Completed, got %s",
			workflowStatus,
		)
	}
}
