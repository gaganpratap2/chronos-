package database

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"

	"chronos/workflow"

	"github.com/joho/godotenv"
)

func TestSaveWorkflow(t *testing.T) {
	ctx := context.Background()

	if err := godotenv.Load("../.env"); err != nil {
		t.Fatalf("load .env: %v", err)
	}

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("connect database: %v", err)
	}
	defer pool.Close()

	const workflowID int64 = 900001

	_, err = pool.Exec(
		ctx,
		`DELETE FROM workflows WHERE id = $1`,
		workflowID,
	)
	if err != nil {
		t.Fatalf("cleanup workflow: %v", err)
	}

	w, err := workflow.NewWorkflow(
		workflowID,
		"test-workflow",
	)
	if err != nil {
		t.Fatalf("create workflow: %v", err)
	}

	if err := SaveWorkflow(ctx, pool, w); err != nil {
		t.Fatalf("save workflow: %v", err)
	}

	var (
		savedID     int64
		savedName   string
		savedStatus string
	)

	err = pool.QueryRow(
		ctx,
		`SELECT id, name, status
		 FROM workflows
		 WHERE id = $1`,
		workflowID,
	).Scan(
		&savedID,
		&savedName,
		&savedStatus,
	)
	if err != nil {
		t.Fatalf("read saved workflow: %v", err)
	}

	if savedID != workflowID {
		t.Fatalf("expected ID %d, got %d", workflowID, savedID)
	}

	if savedName != "test-workflow" {
		t.Fatalf("expected name test-workflow, got %s", savedName)
	}

	if savedStatus != string(workflow.StatusPending) {
		t.Fatalf(
			"expected status %s, got %s",
			workflow.StatusPending,
			savedStatus,
		)
	}

	_, err = pool.Exec(
		ctx,
		`DELETE FROM workflows WHERE id = $1`,
		workflowID,
	)
	if err != nil {
		t.Fatalf("cleanup workflow after test: %v", err)
	}
}

func TestGetWorkflow(t *testing.T) {
	ctx := context.Background()

	if err := godotenv.Load("../.env"); err != nil {
		t.Fatalf("load .env: %v", err)
	}

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("connect database: %v", err)
	}
	defer pool.Close()

	const workflowID int64 = 900002

	_, err = pool.Exec(
		ctx,
		`DELETE FROM workflows WHERE id = $1`,
		workflowID,
	)
	if err != nil {
		t.Fatalf("cleanup before test: %v", err)
	}

	defer pool.Exec(
		context.Background(),
		`DELETE FROM workflows WHERE id = $1`,
		workflowID,
	)

	_, err = pool.Exec(
		ctx,
		`INSERT INTO workflows (id, name, status)
		 VALUES ($1, $2, $3)`,
		workflowID,
		"get-workflow-test",
		workflow.StatusRunning,
	)
	if err != nil {
		t.Fatalf("insert workflow: %v", err)
	}

	got, err := GetWorkflow(ctx, pool, workflowID)
	if err != nil {
		t.Fatalf("get workflow: %v", err)
	}

	if got.ID != workflowID {
		t.Fatalf("expected ID %d, got %d", workflowID, got.ID)
	}

	if got.Name != "get-workflow-test" {
		t.Fatalf(
			"expected name %q, got %q",
			"get-workflow-test",
			got.Name,
		)
	}

	if got.Status != workflow.StatusRunning {
		t.Fatalf(
			"expected status %q, got %q",
			workflow.StatusRunning,
			got.Status,
		)
	}
}

func TestGetWorkflowNotFound(t *testing.T) {
	ctx := context.Background()

	if err := godotenv.Load("../.env"); err != nil {
		t.Fatalf("load .env: %v", err)
	}

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("connect database: %v", err)
	}
	defer pool.Close()

	const workflowID int64 = 999999999

	_, err = GetWorkflow(ctx, pool, workflowID)
	if err == nil {
		t.Fatal("expected error for missing workflow")
	}

	if !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf(
			"expected pgx.ErrNoRows, got %v",
			err,
		)
	}
}

func TestGetWorkflowPreservesStatus(t *testing.T) {
	ctx := context.Background()

	if err := godotenv.Load("../.env"); err != nil {
		t.Fatalf("load .env: %v", err)
	}

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("connect database: %v", err)
	}
	defer pool.Close()

	statuses := []workflow.Status{
		workflow.StatusPending,
		workflow.StatusRunning,
		workflow.StatusCompleted,
		workflow.StatusFailed,
	}

	for i, status := range statuses {
		workflowID := int64(910000 + i)

		_, err = pool.Exec(
			ctx,
			`DELETE FROM workflows WHERE id = $1`,
			workflowID,
		)
		if err != nil {
			t.Fatalf("cleanup before test: %v", err)
		}

		_, err = pool.Exec(
			ctx,
			`INSERT INTO workflows (id, name, status)
			 VALUES ($1, $2, $3)`,
			workflowID,
			"status-test",
			status,
		)
		if err != nil {
			t.Fatalf("insert workflow: %v", err)
		}

		got, err := GetWorkflow(ctx, pool, workflowID)
		if err != nil {
			t.Fatalf(
				"get workflow with status %q: %v",
				status,
				err,
			)
		}

		if got.Status != status {
			t.Fatalf(
				"expected status %q, got %q",
				status,
				got.Status,
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
}

func TestSaveTask(t *testing.T) {
	ctx := context.Background()

	if err := godotenv.Load("../.env"); err != nil {
		t.Fatalf("load .env: %v", err)
	}

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("connect database: %v", err)
	}
	defer pool.Close()

	const workflowID int64 = 920001
	const taskID int64 = 930001

	_, err = pool.Exec(
		ctx,
		`DELETE FROM workflows WHERE id = $1`,
		workflowID,
	)
	if err != nil {
		t.Fatalf("cleanup before test: %v", err)
	}

	defer pool.Exec(
		context.Background(),
		`DELETE FROM workflows WHERE id = $1`,
		workflowID,
	)

	_, err = pool.Exec(
		ctx,
		`INSERT INTO workflows (id, name, status)
		 VALUES ($1, $2, $3)`,
		workflowID,
		"task-test-workflow",
		workflow.StatusPending,
	)
	if err != nil {
		t.Fatalf("insert workflow: %v", err)
	}

	task, err := workflow.NewTask(
		taskID,
		workflowID,
		"send-email",
	)
	if err != nil {
		t.Fatalf("create task: %v", err)
	}

	if err := SaveTask(ctx, pool, task); err != nil {
		t.Fatalf("save task: %v", err)
	}

	var (
		gotID         int64
		gotWorkflowID int64
		gotName       string
		gotStatus     workflow.TaskStatus
	)

	err = pool.QueryRow(
		ctx,
		`SELECT id, workflow_id, name, status
		 FROM workflow_tasks
		 WHERE id = $1`,
		taskID,
	).Scan(
		&gotID,
		&gotWorkflowID,
		&gotName,
		&gotStatus,
	)
	if err != nil {
		t.Fatalf("read saved task: %v", err)
	}

	if gotID != taskID {
		t.Fatalf(
			"expected task ID %d, got %d",
			taskID,
			gotID,
		)
	}

	if gotWorkflowID != workflowID {
		t.Fatalf(
			"expected workflow ID %d, got %d",
			workflowID,
			gotWorkflowID,
		)
	}

	if gotName != "send-email" {
		t.Fatalf(
			"expected task name %q, got %q",
			"send-email",
			gotName,
		)
	}

	if gotStatus != workflow.TaskPending {
		t.Fatalf(
			"expected task status %q, got %q",
			workflow.TaskPending,
			gotStatus,
		)
	}
}

func TestGetTask(t *testing.T) {
	ctx := context.Background()

	if err := godotenv.Load("../.env"); err != nil {
		t.Fatalf("load .env: %v", err)
	}

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("connect database: %v", err)
	}
	defer pool.Close()

	const workflowID int64 = 940001
	const taskID int64 = 950001

	_, err = pool.Exec(
		ctx,
		`DELETE FROM workflows WHERE id = $1`,
		workflowID,
	)
	if err != nil {
		t.Fatalf("cleanup before test: %v", err)
	}

	defer pool.Exec(
		context.Background(),
		`DELETE FROM workflows WHERE id = $1`,
		workflowID,
	)

	_, err = pool.Exec(
		ctx,
		`INSERT INTO workflows (id, name, status)
		 VALUES ($1, $2, $3)`,
		workflowID,
		"get-task-workflow",
		workflow.StatusRunning,
	)
	if err != nil {
		t.Fatalf("insert workflow: %v", err)
	}

	_, err = pool.Exec(
		ctx,
		`INSERT INTO workflow_tasks (
			id,
			workflow_id,
			name,
			status
		)
		VALUES ($1, $2, $3, $4)`,
		taskID,
		workflowID,
		"process-payment",
		workflow.TaskRunning,
	)
	if err != nil {
		t.Fatalf("insert task: %v", err)
	}

	got, err := GetTask(ctx, pool, taskID)
	if err != nil {
		t.Fatalf("get task: %v", err)
	}

	if got.ID != taskID {
		t.Fatalf(
			"expected task ID %d, got %d",
			taskID,
			got.ID,
		)
	}

	if got.WorkflowID != workflowID {
		t.Fatalf(
			"expected workflow ID %d, got %d",
			workflowID,
			got.WorkflowID,
		)
	}

	if got.Name != "process-payment" {
		t.Fatalf(
			"expected task name %q, got %q",
			"process-payment",
			got.Name,
		)
	}

	if got.Status != workflow.TaskRunning {
		t.Fatalf(
			"expected task status %q, got %q",
			workflow.TaskRunning,
			got.Status,
		)
	}
}

func TestGetTaskNotFound(t *testing.T) {
	ctx := context.Background()

	if err := godotenv.Load("../.env"); err != nil {
		t.Fatalf("load .env: %v", err)
	}

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("connect database: %v", err)
	}
	defer pool.Close()

	const taskID int64 = 999999999

	_, err = GetTask(ctx, pool, taskID)
	if err == nil {
		t.Fatal("expected error for missing task")
	}

	if !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf(
			"expected pgx.ErrNoRows, got %v",
			err,
		)
	}
}

func TestSaveDependency(t *testing.T) {
	ctx := context.Background()

	if err := godotenv.Load("../.env"); err != nil {
		t.Fatalf("load .env: %v", err)
	}

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("connect database: %v", err)
	}
	defer pool.Close()

	const workflowID int64 = 960001
	const taskAID int64 = 970001
	const taskBID int64 = 970002

	_, err = pool.Exec(
		ctx,
		`DELETE FROM workflows WHERE id = $1`,
		workflowID,
	)
	if err != nil {
		t.Fatalf("cleanup before test: %v", err)
	}

	defer pool.Exec(
		context.Background(),
		`DELETE FROM workflows WHERE id = $1`,
		workflowID,
	)

	_, err = pool.Exec(
		ctx,
		`INSERT INTO workflows (id, name, status)
		 VALUES ($1, $2, $3)`,
		workflowID,
		"dependency-test",
		workflow.StatusPending,
	)
	if err != nil {
		t.Fatalf("insert workflow: %v", err)
	}

	_, err = pool.Exec(
		ctx,
		`INSERT INTO workflow_tasks (
			id,
			workflow_id,
			name,
			status
		)
		VALUES
			($1, $3, 'task-a', $4),
			($2, $3, 'task-b', $4)`,
		taskAID,
		taskBID,
		workflowID,
		workflow.TaskPending,
	)
	if err != nil {
		t.Fatalf("insert tasks: %v", err)
	}

	dependency, err := workflow.NewDependency(
		taskBID,
		taskAID,
	)
	if err != nil {
		t.Fatalf("create dependency: %v", err)
	}

	if err := SaveDependency(ctx, pool, dependency); err != nil {
		t.Fatalf("save dependency: %v", err)
	}

	var (
		gotTaskID          int64
		gotDependsOnTaskID int64
	)

	err = pool.QueryRow(
		ctx,
		`SELECT task_id, depends_on_task_id
		 FROM task_dependencies
		 WHERE task_id = $1
		   AND depends_on_task_id = $2`,
		taskBID,
		taskAID,
	).Scan(
		&gotTaskID,
		&gotDependsOnTaskID,
	)
	if err != nil {
		t.Fatalf("read saved dependency: %v", err)
	}

	if gotTaskID != taskBID {
		t.Fatalf(
			"expected task ID %d, got %d",
			taskBID,
			gotTaskID,
		)
	}

	if gotDependsOnTaskID != taskAID {
		t.Fatalf(
			"expected dependency task ID %d, got %d",
			taskAID,
			gotDependsOnTaskID,
		)
	}
}

func TestSaveDependencyRejectsDuplicate(t *testing.T) {
	ctx := context.Background()

	if err := godotenv.Load("../.env"); err != nil {
		t.Fatalf("load .env: %v", err)
	}

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("connect database: %v", err)
	}
	defer pool.Close()

	const workflowID int64 = 980001
	const taskAID int64 = 990001
	const taskBID int64 = 990002

	_, err = pool.Exec(
		ctx,
		`DELETE FROM workflows WHERE id = $1`,
		workflowID,
	)
	if err != nil {
		t.Fatalf("cleanup before test: %v", err)
	}

	defer pool.Exec(
		context.Background(),
		`DELETE FROM workflows WHERE id = $1`,
		workflowID,
	)

	_, err = pool.Exec(
		ctx,
		`INSERT INTO workflows (id, name, status)
		 VALUES ($1, $2, $3)`,
		workflowID,
		"duplicate-dependency-test",
		workflow.StatusPending,
	)
	if err != nil {
		t.Fatalf("insert workflow: %v", err)
	}

	_, err = pool.Exec(
		ctx,
		`INSERT INTO workflow_tasks (
			id,
			workflow_id,
			name,
			status
		)
		VALUES
			($1, $3, 'task-a', $4),
			($2, $3, 'task-b', $4)`,
		taskAID,
		taskBID,
		workflowID,
		workflow.TaskPending,
	)
	if err != nil {
		t.Fatalf("insert tasks: %v", err)
	}

	dependency, err := workflow.NewDependency(
		taskBID,
		taskAID,
	)
	if err != nil {
		t.Fatalf("create dependency: %v", err)
	}

	if err := SaveDependency(ctx, pool, dependency); err != nil {
		t.Fatalf("save first dependency: %v", err)
	}

	err = SaveDependency(ctx, pool, dependency)
	if err == nil {
		t.Fatal("expected duplicate dependency error")
	}
}

func TestSaveDependencyRejectsMissingTask(t *testing.T) {
	ctx := context.Background()

	if err := godotenv.Load("../.env"); err != nil {
		t.Fatalf("load .env: %v", err)
	}

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("connect database: %v", err)
	}
	defer pool.Close()

	const workflowID int64 = 991000
	const missingTaskID int64 = 991001
	const existingTaskID int64 = 991002

	_, err = pool.Exec(
		ctx,
		`DELETE FROM workflows WHERE id = $1`,
		workflowID,
	)
	if err != nil {
		t.Fatalf("cleanup before test: %v", err)
	}

	defer pool.Exec(
		context.Background(),
		`DELETE FROM workflows WHERE id = $1`,
		workflowID,
	)

	_, err = pool.Exec(
		ctx,
		`INSERT INTO workflows (id, name, status)
		 VALUES ($1, $2, $3)`,
		workflowID,
		"missing-task-dependency-test",
		workflow.StatusPending,
	)
	if err != nil {
		t.Fatalf("insert workflow: %v", err)
	}

	_, err = pool.Exec(
		ctx,
		`INSERT INTO workflow_tasks (
			id,
			workflow_id,
			name,
			status
		)
		VALUES ($1, $2, $3, $4)`,
		existingTaskID,
		workflowID,
		"existing-task",
		workflow.TaskPending,
	)
	if err != nil {
		t.Fatalf("insert existing task: %v", err)
	}

	dependency, err := workflow.NewDependency(
		existingTaskID,
		missingTaskID,
	)
	if err != nil {
		t.Fatalf("create dependency: %v", err)
	}

	err = SaveDependency(ctx, pool, dependency)
	if err == nil {
		t.Fatal("expected foreign-key error")
	}
}

func TestGetWorkflowTasks(t *testing.T) {
	ctx := context.Background()

	if err := godotenv.Load("../.env"); err != nil {
		t.Fatalf("load .env: %v", err)
	}

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("connect database: %v", err)
	}
	defer pool.Close()

	const workflowID int64 = 992001
	const taskAID int64 = 992002
	const taskBID int64 = 992003
	const taskCID int64 = 992004

	_, err = pool.Exec(
		ctx,
		`DELETE FROM workflows WHERE id = $1`,
		workflowID,
	)
	if err != nil {
		t.Fatalf("cleanup before test: %v", err)
	}

	defer pool.Exec(
		context.Background(),
		`DELETE FROM workflows WHERE id = $1`,
		workflowID,
	)

	_, err = pool.Exec(
		ctx,
		`INSERT INTO workflows (id, name, status)
		 VALUES ($1, $2, $3)`,
		workflowID,
		"get-tasks-test",
		workflow.StatusRunning,
	)
	if err != nil {
		t.Fatalf("insert workflow: %v", err)
	}

	_, err = pool.Exec(
		ctx,
		`INSERT INTO workflow_tasks (
			id,
			workflow_id,
			name,
			status
		)
		VALUES
			($1, $4, $5, $6),
			($2, $4, $7, $8),
			($3, $4, $9, $10)`,
		taskAID,
		taskBID,
		taskCID,
		workflowID,
		"task-a",
		workflow.TaskCompleted,
		"task-b",
		workflow.TaskRunning,
		"task-c",
		workflow.TaskPending,
	)
	if err != nil {
		t.Fatalf("insert tasks: %v", err)
	}

	tasks, err := GetWorkflowTasks(ctx, pool, workflowID)
	if err != nil {
		t.Fatalf("get workflow tasks: %v", err)
	}

	if len(tasks) != 3 {
		t.Fatalf(
			"expected 3 tasks, got %d",
			len(tasks),
		)
	}

	expectedIDs := []int64{
		taskAID,
		taskBID,
		taskCID,
	}

	expectedNames := []string{
		"task-a",
		"task-b",
		"task-c",
	}

	expectedStatuses := []workflow.TaskStatus{
		workflow.TaskCompleted,
		workflow.TaskRunning,
		workflow.TaskPending,
	}

	for i, task := range tasks {
		if task.ID != expectedIDs[i] {
			t.Fatalf(
				"task %d: expected ID %d, got %d",
				i,
				expectedIDs[i],
				task.ID,
			)
		}

		if task.WorkflowID != workflowID {
			t.Fatalf(
				"task %d: expected workflow ID %d, got %d",
				i,
				workflowID,
				task.WorkflowID,
			)
		}

		if task.Name != expectedNames[i] {
			t.Fatalf(
				"task %d: expected name %q, got %q",
				i,
				expectedNames[i],
				task.Name,
			)
		}

		if task.Status != expectedStatuses[i] {
			t.Fatalf(
				"task %d: expected status %q, got %q",
				i,
				expectedStatuses[i],
				task.Status,
			)
		}
	}
}

func TestGetWorkflowDependencies(t *testing.T) {
	ctx := context.Background()

	if err := godotenv.Load("../.env"); err != nil {
		t.Fatalf("load .env: %v", err)
	}

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("connect database: %v", err)
	}
	defer pool.Close()

	const workflowID int64 = 993001
	const taskAID int64 = 993002
	const taskBID int64 = 993003
	const taskCID int64 = 993004

	_, err = pool.Exec(
		ctx,
		`DELETE FROM workflows WHERE id = $1`,
		workflowID,
	)
	if err != nil {
		t.Fatalf("cleanup before test: %v", err)
	}

	defer pool.Exec(
		context.Background(),
		`DELETE FROM workflows WHERE id = $1`,
		workflowID,
	)

	_, err = pool.Exec(
		ctx,
		`INSERT INTO workflows (id, name, status)
		 VALUES ($1, $2, $3)`,
		workflowID,
		"get-dependencies-test",
		workflow.StatusPending,
	)
	if err != nil {
		t.Fatalf("insert workflow: %v", err)
	}

	_, err = pool.Exec(
		ctx,
		`INSERT INTO workflow_tasks (
			id,
			workflow_id,
			name,
			status
		)
		VALUES
			($1, $4, 'task-a', $5),
			($2, $4, 'task-b', $5),
			($3, $4, 'task-c', $5)`,
		taskAID,
		taskBID,
		taskCID,
		workflowID,
		workflow.TaskPending,
	)
	if err != nil {
		t.Fatalf("insert tasks: %v", err)
	}

	dependencies := []workflow.Dependency{
		{
			TaskID:          taskBID,
			DependsOnTaskID: taskAID,
		},
		{
			TaskID:          taskCID,
			DependsOnTaskID: taskAID,
		},
		{
			TaskID:          taskCID,
			DependsOnTaskID: taskBID,
		},
	}

	for _, dependency := range dependencies {
		if err := SaveDependency(
			ctx,
			pool,
			dependency,
		); err != nil {
			t.Fatalf(
				"save dependency %d -> %d: %v",
				dependency.DependsOnTaskID,
				dependency.TaskID,
				err,
			)
		}
	}

	got, err := GetWorkflowDependencies(
		ctx,
		pool,
		workflowID,
	)
	if err != nil {
		t.Fatalf("get workflow dependencies: %v", err)
	}

	if len(got) != 3 {
		t.Fatalf(
			"expected 3 dependencies, got %d",
			len(got),
		)
	}

	expected := []workflow.Dependency{
		{
			TaskID:          taskBID,
			DependsOnTaskID: taskAID,
		},
		{
			TaskID:          taskCID,
			DependsOnTaskID: taskAID,
		},
		{
			TaskID:          taskCID,
			DependsOnTaskID: taskBID,
		},
	}

	for i, dependency := range got {
		if dependency != expected[i] {
			t.Fatalf(
				"dependency %d: expected %+v, got %+v",
				i,
				expected[i],
				dependency,
			)
		}
	}
}
