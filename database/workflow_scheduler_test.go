package database

import (
	"context"
	"testing"

	"chronos/workflow"

	"github.com/joho/godotenv"
)

func TestGetRunnableTasks(t *testing.T) {
	if err := godotenv.Load("../.env"); err != nil {
		t.Logf("warning: .env not loaded: %v", err)
	}

	ctx := context.Background()

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("connect database: %v", err)
	}
	defer pool.Close()

	workflowID := int64(995001)
	taskAID := int64(995001)
	taskBID := int64(995002)
	taskCID := int64(995003)

	_, err = pool.Exec(
		ctx,
		`DELETE FROM workflows WHERE id = $1`,
		workflowID,
	)
	if err != nil {
		t.Fatalf("cleanup before test: %v", err)
	}

	w, err := workflow.NewWorkflow(
		workflowID,
		"runnable-test",
	)
	if err != nil {
		t.Fatalf("create workflow: %v", err)
	}

	if err := SaveWorkflow(ctx, pool, w); err != nil {
		t.Fatalf("save workflow: %v", err)
	}

	taskA, err := workflow.NewTask(
		taskAID,
		workflowID,
		"A",
	)
	if err != nil {
		t.Fatalf("create task A: %v", err)
	}

	taskB, err := workflow.NewTask(
		taskBID,
		workflowID,
		"B",
	)
	if err != nil {
		t.Fatalf("create task B: %v", err)
	}

	taskC, err := workflow.NewTask(
		taskCID,
		workflowID,
		"C",
	)
	if err != nil {
		t.Fatalf("create task C: %v", err)
	}

	taskA.Status = workflow.TaskCompleted

	if err := SaveTask(ctx, pool, taskA); err != nil {
		t.Fatalf("save task A: %v", err)
	}

	if err := SaveTask(ctx, pool, taskB); err != nil {
		t.Fatalf("save task B: %v", err)
	}

	if err := SaveTask(ctx, pool, taskC); err != nil {
		t.Fatalf("save task C: %v", err)
	}

	dependencyB := workflow.Dependency{
		TaskID:          taskBID,
		DependsOnTaskID: taskAID,
	}

	dependencyC := workflow.Dependency{
		TaskID:          taskCID,
		DependsOnTaskID: taskBID,
	}

	if err := SaveDependency(ctx, pool, dependencyB); err != nil {
		t.Fatalf("save dependency B: %v", err)
	}

	if err := SaveDependency(ctx, pool, dependencyC); err != nil {
		t.Fatalf("save dependency C: %v", err)
	}

	tasks, err := GetRunnableTasks(ctx, pool)
	if err != nil {
		t.Fatalf("get runnable tasks: %v", err)
	}

	foundB := false
	foundC := false

	for _, task := range tasks {
		switch task.ID {
		case taskBID:
			foundB = true
		case taskCID:
			foundC = true
		}
	}

	if !foundB {
		t.Fatal("expected task B to be runnable")
	}

	if foundC {
		t.Fatal("expected task C to remain not runnable")
	}

	// Clean up test data so it cannot affect other tests.
	_, err = pool.Exec(
		ctx,
		`DELETE FROM workflows WHERE id = $1`,
		workflowID,
	)
	if err != nil {
		t.Fatalf("cleanup after test: %v", err)
	}
}

func TestClaimRunnableTask(t *testing.T) {
	if err := godotenv.Load("../.env"); err != nil {
		t.Logf("warning: .env not loaded: %v", err)
	}

	ctx := context.Background()

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("connect database: %v", err)
	}
	defer pool.Close()

	const workflowID = int64(996101)
	const taskID = int64(996101)
	const workflowName = "claim-test-996101"

	// Clean up anything left by a previous test run.
	_, err = pool.Exec(
		ctx,
		`DELETE FROM workflows WHERE id = $1 OR name = $2`,
		workflowID,
		workflowName,
	)
	if err != nil {
		t.Fatalf("cleanup before test: %v", err)
	}

	// Clean up stale test data that could otherwise be selected
	// by ClaimRunnableTask().
	_, err = pool.Exec(
		ctx,
		`
		DELETE FROM workflows
		WHERE name LIKE 'claim-test-%'
		`,
	)
	if err != nil {
		t.Fatalf("cleanup stale claim tests: %v", err)
	}

	w, err := workflow.NewWorkflow(
		workflowID,
		workflowName,
	)
	if err != nil {
		t.Fatalf("create workflow: %v", err)
	}

	if err := SaveWorkflow(ctx, pool, w); err != nil {
		t.Fatalf("save workflow: %v", err)
	}

	task, err := workflow.NewTask(
		taskID,
		workflowID,
		"claim-me",
	)
	if err != nil {
		t.Fatalf("create task: %v", err)
	}

	if err := SaveTask(ctx, pool, task); err != nil {
		t.Fatalf("save task: %v", err)
	}

	claimed, err := ClaimRunnableTask(ctx, pool)
	if err != nil {
		t.Fatalf("claim task: %v", err)
	}

	if claimed == nil {
		t.Fatal("expected a task to be claimed")
	}

	if claimed.ID != taskID {
		t.Fatalf(
			"expected task %d, got %d",
			taskID,
			claimed.ID,
		)
	}

	var status string

	err = pool.QueryRow(
		ctx,
		`SELECT status FROM workflow_tasks WHERE id = $1`,
		taskID,
	).Scan(&status)
	if err != nil {
		t.Fatalf("read task status: %v", err)
	}

	if status != string(workflow.TaskReady) {
		t.Fatalf(
			"expected task status Ready, got %s",
			status,
		)
	}

	claimedAgain, err := ClaimRunnableTask(ctx, pool)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}

	if claimedAgain != nil && claimedAgain.ID == taskID {
		t.Fatal("expected task to not be claimed twice")
	}

	// Clean up this test's data.
	_, err = pool.Exec(
		ctx,
		`DELETE FROM workflows WHERE id = $1`,
		workflowID,
	)
	if err != nil {
		t.Fatalf("cleanup after test: %v", err)
	}
}

func TestClaimRunnableTaskConcurrently(t *testing.T) {
	if err := godotenv.Load("../.env"); err != nil {
		t.Logf("warning: .env not loaded: %v", err)
	}

	ctx := context.Background()

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("connect database: %v", err)
	}

	defer pool.Close()

	const workflowID = int64(997001)
	const taskAID = int64(997001)
	const taskBID = int64(997002)
	const workflowName = "concurrent-claim-test"

	_, err = pool.Exec(ctx,
		`DELETE FROM workflows WHERE id = $1`,
		workflowID,
	)

	if err != nil {
		t.Fatalf("cleanup before test: %v", err)
	}

	w, err := workflow.NewWorkflow(workflowID, workflowName)
	if err != nil {
		t.Fatalf("create workflow: %v", err)
	}

	if err := SaveWorkflow(ctx, pool, w); err != nil {
		t.Fatalf("save workflow: %v", err)
	}

	taskA, err := workflow.NewTask(taskAID, workflowID, "task-a")
	if err != nil {
		t.Fatalf("create task A: %v", err)
	}

	taskB, err := workflow.NewTask(taskBID, workflowID, "task-b")
	if err != nil {
		t.Fatalf("create task B: %v", err)
	}

	if err := SaveTask(ctx, pool, taskA); err != nil {
		t.Fatalf("save task A: %v", err)
	}

	if err := SaveTask(ctx, pool, taskB); err != nil {
		t.Fatalf("save task A: %v", err)
	}

	start := make(chan struct{})
	result := make(chan *RunnableTask, 2)
	errs := make(chan error, 2)

	claim := func() {
		<-start

		task, err := ClaimRunnableTask(ctx, pool)
		if err != nil {
			errs <- err
			return
		}

		result <- task
	}

	go claim()
	go claim()

	close(start)

	var claimed []*RunnableTask

	for i := 0; i < 2; i++ {
		select {
		case err := <-errs:
			t.Fatalf("claim task: %v", err)

		case task := <-result:
			if task == nil {
				t.Fatal("expected a task to be claimed")
			}
			claimed = append(claimed, task)
		}
	}

	if len(claimed) != 2 {
		t.Fatalf(
			"expected 2 claimed tasks, got %d",
			len(claimed),
		)
	}

	if claimed[0].ID == claimed[1].ID {
		t.Fatalf(
			"same task was claimed twice: %d",
			claimed[0].ID,
		)
	}

	if claimed[0].ID != taskAID && claimed[0].ID != taskBID {
		t.Fatalf(
			"unexpected first task: %d",
			claimed[0].ID,
		)
	}

	if claimed[1].ID != taskAID && claimed[1].ID != taskBID {
		t.Fatalf(
			"unexpected second task: %d",
			claimed[1].ID,
		)
	}

	var readyCount int

	err = pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM workflow_tasks
		WHERE workflow_id = $1
		  AND status = 'Ready'
		`,
		workflowID,
	).Scan(&readyCount)
	if err != nil {
		t.Fatalf("count ready tasks: %v", err)
	}

	if readyCount != 2 {
		t.Fatalf(
			"expected 2 Ready tasks, got %d",
			readyCount,
		)
	}

	_, err = pool.Exec(
		ctx,
		`DELETE FROM workflows WHERE id = $1`,
		workflowID,
	)
	if err != nil {
		t.Fatalf("cleanup after test: %v", err)
	}
}
