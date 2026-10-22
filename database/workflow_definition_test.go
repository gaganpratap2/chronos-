package database

import (
	"context"
	"errors"
	"testing"

	"chronos/workflow"

	"github.com/joho/godotenv"
)

func TestCreateWorkflow(t *testing.T) {
	ctx := context.Background()

	if err := godotenv.Load("../.env"); err != nil {
		t.Fatalf("load .env: %v", err)
	}

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("connect database: %v", err)
	}
	defer pool.Close()

	const workflowID int64 = 994001
	const taskAID int64 = 994002
	const taskBID int64 = 994003

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

	w, err := workflow.NewWorkflow(
		workflowID,
		"transactional-workflow",
	)
	if err != nil {
		t.Fatalf("create workflow: %v", err)
	}

	taskA, err := workflow.NewTask(
		taskAID,
		workflowID,
		"task-a",
	)
	if err != nil {
		t.Fatalf("create task A: %v", err)
	}

	taskB, err := workflow.NewTask(
		taskBID,
		workflowID,
		"task-b",
	)
	if err != nil {
		t.Fatalf("create task B: %v", err)
	}

	dependency, err := workflow.NewDependency(
		taskBID,
		taskAID,
	)
	if err != nil {
		t.Fatalf("create dependency: %v", err)
	}

	definition := WorkflowDefinition{
		Workflow: w,
		Tasks: []workflow.Task{
			taskA,
			taskB,
		},
		Dependencies: []workflow.Dependency{
			dependency,
		},
	}

	if err := CreateWorkflow(ctx, pool, definition); err != nil {
		t.Fatalf("create workflow: %v", err)
	}

	var workflowName string

	err = pool.QueryRow(
		ctx,
		`SELECT name
		 FROM workflows
		 WHERE id = $1`,
		workflowID,
	).Scan(&workflowName)
	if err != nil {
		t.Fatalf("verify workflow: %v", err)
	}

	if workflowName != "transactional-workflow" {
		t.Fatalf(
			"expected workflow name %q, got %q",
			"transactional-workflow",
			workflowName,
		)
	}

	var taskCount int

	err = pool.QueryRow(
		ctx,
		`SELECT COUNT(*)
		 FROM workflow_tasks
		 WHERE workflow_id = $1`,
		workflowID,
	).Scan(&taskCount)
	if err != nil {
		t.Fatalf("count tasks: %v", err)
	}

	if taskCount != 2 {
		t.Fatalf(
			"expected 2 tasks, got %d",
			taskCount,
		)
	}

	var dependencyCount int

	err = pool.QueryRow(
		ctx,
		`SELECT COUNT(*)
		 FROM task_dependencies td
		 JOIN workflow_tasks t
			 ON t.id = td.task_id
		 WHERE t.workflow_id = $1`,
		workflowID,
	).Scan(&dependencyCount)
	if err != nil {
		t.Fatalf("count dependencies: %v", err)
	}

	if dependencyCount != 1 {
		t.Fatalf(
			"expected 1 dependency, got %d",
			dependencyCount,
		)
	}
}

func TestCreateWorkflowRollsBackOnDependencyFailure(t *testing.T) {
	ctx := context.Background()

	if err := godotenv.Load("../.env"); err != nil {
		t.Fatalf("load .env: %v", err)
	}

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("connect database: %v", err)
	}
	defer pool.Close()

	const workflowID int64 = 995001
	const taskID int64 = 995002
	const missingTaskID int64 = 995003

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

	w, err := workflow.NewWorkflow(
		workflowID,
		"rollback-test",
	)
	if err != nil {
		t.Fatalf("create workflow: %v", err)
	}

	task, err := workflow.NewTask(
		taskID,
		workflowID,
		"task-a",
	)
	if err != nil {
		t.Fatalf("create task: %v", err)
	}

	dependency, err := workflow.NewDependency(
		taskID,
		missingTaskID,
	)
	if err != nil {
		t.Fatalf("create dependency: %v", err)
	}

	definition := WorkflowDefinition{
		Workflow: w,
		Tasks: []workflow.Task{
			task,
		},
		Dependencies: []workflow.Dependency{
			dependency,
		},
	}

	err = CreateWorkflow(ctx, pool, definition)
	if err == nil {
		t.Fatal("expected workflow creation to fail")
	}

	var workflowCount int

	err = pool.QueryRow(
		ctx,
		`SELECT COUNT(*)
		 FROM workflows
		 WHERE id = $1`,
		workflowID,
	).Scan(&workflowCount)
	if err != nil {
		t.Fatalf("count workflow: %v", err)
	}

	if workflowCount != 0 {
		t.Fatalf(
			"expected workflow rollback, found %d workflow(s)",
			workflowCount,
		)
	}

	var taskCount int

	err = pool.QueryRow(
		ctx,
		`SELECT COUNT(*)
		 FROM workflow_tasks
		 WHERE workflow_id = $1`,
		workflowID,
	).Scan(&taskCount)
	if err != nil {
		t.Fatalf("count tasks: %v", err)
	}

	if taskCount != 0 {
		t.Fatalf(
			"expected task rollback, found %d task(s)",
			taskCount,
		)
	}
}

// Valid workflow
func TestValidateWorkflowDefinition(t *testing.T) {
	w, err := workflow.NewWorkflow(
		10001,
		"valid-workflow",
	)
	if err != nil {
		t.Fatalf("create workflow: %v", err)
	}

	taskA, err := workflow.NewTask(
		10002,
		w.ID,
		"task-a",
	)
	if err != nil {
		t.Fatalf("create task A: %v", err)
	}

	taskB, err := workflow.NewTask(
		10003,
		w.ID,
		"task-b",
	)
	if err != nil {
		t.Fatalf("create task B: %v", err)
	}

	dependency, err := workflow.NewDependency(
		taskB.ID,
		taskA.ID,
	)
	if err != nil {
		t.Fatalf("create dependency: %v", err)
	}

	definition := WorkflowDefinition{
		Workflow: w,
		Tasks: []workflow.Task{
			taskA,
			taskB,
		},
		Dependencies: []workflow.Dependency{
			dependency,
		},
	}

	if err := ValidateWorkflowDefinition(definition); err != nil {
		t.Fatalf(
			"expected valid definition, got %v",
			err,
		)
	}
}

// Reject task from another workflow

func TestValidateWorkflowDefinitionRejectsWrongWorkflowTask(t *testing.T) {
	w, err := workflow.NewWorkflow(
		11001,
		"workflow-a",
	)
	if err != nil {
		t.Fatalf("create workflow: %v", err)
	}

	task, err := workflow.NewTask(
		11002,
		99999,
		"wrong-workflow-task",
	)
	if err != nil {
		t.Fatalf("create task: %v", err)
	}

	definition := WorkflowDefinition{
		Workflow: w,
		Tasks: []workflow.Task{
			task,
		},
	}

	if err := ValidateWorkflowDefinition(definition); err == nil {
		t.Fatal("expected wrong workflow task to be rejected")
	}
}

// Reject missing dependency task

func TestValidateWorkflowDefinitionRejectsMissingDependencyTask(t *testing.T) {
	w, err := workflow.NewWorkflow(
		12001,
		"missing-task-workflow",
	)
	if err != nil {
		t.Fatalf("create workflow: %v", err)
	}

	taskA, err := workflow.NewTask(
		12002,
		w.ID,
		"task-a",
	)
	if err != nil {
		t.Fatalf("create task: %v", err)
	}

	dependency, err := workflow.NewDependency(
		12002,
		99999,
	)
	if err != nil {
		t.Fatalf("create dependency: %v", err)
	}

	definition := WorkflowDefinition{
		Workflow: w,
		Tasks: []workflow.Task{
			taskA,
		},
		Dependencies: []workflow.Dependency{
			dependency,
		},
	}

	if err := ValidateWorkflowDefinition(definition); err == nil {
		t.Fatal("expected missing dependency task to be rejected")
	}
}

// Reject cycle
func TestValidateWorkflowDefinitionRejectsCycle(t *testing.T) {
	w, err := workflow.NewWorkflow(
		13001,
		"cycle-workflow",
	)
	if err != nil {
		t.Fatalf("create workflow: %v", err)
	}

	taskA, _ := workflow.NewTask(13002, w.ID, "task-a")
	taskB, _ := workflow.NewTask(13003, w.ID, "task-b")
	taskC, _ := workflow.NewTask(13004, w.ID, "task-c")

	dependencies := []workflow.Dependency{
		{
			TaskID:          taskB.ID,
			DependsOnTaskID: taskA.ID,
		},
		{
			TaskID:          taskC.ID,
			DependsOnTaskID: taskB.ID,
		},
		{
			TaskID:          taskA.ID,
			DependsOnTaskID: taskC.ID,
		},
	}

	definition := WorkflowDefinition{
		Workflow: w,
		Tasks: []workflow.Task{
			taskA,
			taskB,
			taskC,
		},
		Dependencies: dependencies,
	}

	err = ValidateWorkflowDefinition(definition)
	if !errors.Is(err, workflow.ErrCycleDetected) {
		t.Fatalf(
			"expected ErrCycleDetected, got %v",
			err,
		)
	}
}

// invalid definition never reaches DB

func TestCreateWorkflowRejectsCycleBeforePersistence(t *testing.T) {
	ctx := context.Background()

	if err := godotenv.Load("../.env"); err != nil {
		t.Fatalf("load .env: %v", err)
	}

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("connect database: %v", err)
	}
	defer pool.Close()

	const workflowID int64 = 996001

	_, err = pool.Exec(
		ctx,
		`DELETE FROM workflows WHERE id = $1`,
		workflowID,
	)
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	defer pool.Exec(
		context.Background(),
		`DELETE FROM workflows WHERE id = $1`,
		workflowID,
	)

	w, err := workflow.NewWorkflow(
		workflowID,
		"cycle-persistence-test",
	)
	if err != nil {
		t.Fatalf("create workflow: %v", err)
	}

	taskA, _ := workflow.NewTask(996002, workflowID, "a")
	taskB, _ := workflow.NewTask(996003, workflowID, "b")

	definition := WorkflowDefinition{
		Workflow: w,
		Tasks: []workflow.Task{
			taskA,
			taskB,
		},
		Dependencies: []workflow.Dependency{
			{
				TaskID:          taskB.ID,
				DependsOnTaskID: taskA.ID,
			},
			{
				TaskID:          taskA.ID,
				DependsOnTaskID: taskB.ID,
			},
		},
	}

	err = CreateWorkflow(ctx, pool, definition)
	if !errors.Is(err, workflow.ErrCycleDetected) {
		t.Fatalf(
			"expected ErrCycleDetected, got %v",
			err,
		)
	}

	var count int

	err = pool.QueryRow(
		ctx,
		`SELECT COUNT(*)
		 FROM workflows
		 WHERE id = $1`,
		workflowID,
	).Scan(&count)
	if err != nil {
		t.Fatalf("check workflow persistence: %v", err)
	}

	if count != 0 {
		t.Fatalf(
			"expected invalid workflow not to be persisted, found %d",
			count,
		)
	}
}
