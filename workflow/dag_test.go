package workflow

import (
	"errors"
	"reflect"
	"testing"
)

func TestHasCycleLinearGraph(t *testing.T) {
	tasks := []Task{
		{ID: 1},
		{ID: 2},
		{ID: 3},
	}

	dependencies := []Dependency{
		{TaskID: 2, DependsOnTaskID: 1},
		{TaskID: 3, DependsOnTaskID: 2},
	}

	if HasCycle(tasks, dependencies) {
		t.Fatal("expected linear graph to be acyclic")
	}
}

func TestHasCycleBranchingFanInGraph(t *testing.T) {
	tasks := []Task{
		{ID: 1},
		{ID: 2},
		{ID: 3},
		{ID: 4},
	}

	dependencies := []Dependency{
		{TaskID: 2, DependsOnTaskID: 1},
		{TaskID: 3, DependsOnTaskID: 1},
		{TaskID: 4, DependsOnTaskID: 2},
		{TaskID: 4, DependsOnTaskID: 3},
	}

	if HasCycle(tasks, dependencies) {
		t.Fatal("expected branching/fan-in graph to be acyclic")
	}
}

func TestHasCycleDetectsCycle(t *testing.T) {
	tasks := []Task{
		{ID: 1},
		{ID: 2},
		{ID: 3},
	}

	dependencies := []Dependency{
		{TaskID: 2, DependsOnTaskID: 1},
		{TaskID: 3, DependsOnTaskID: 2},
		{TaskID: 1, DependsOnTaskID: 3},
	}

	if !HasCycle(tasks, dependencies) {
		t.Fatal("expected cycle to be detected")
	}
}

func TestHasCycleEmptyGraph(t *testing.T) {
	if HasCycle(nil, nil) {
		t.Fatal("expected empty graph to be acyclic")
	}
}

func TestHasCycleDisconnectedGraph(t *testing.T) {
	tasks := []Task{
		{ID: 1},
		{ID: 2},
		{ID: 3},
		{ID: 4},
	}

	dependencies := []Dependency{
		{TaskID: 2, DependsOnTaskID: 1},
		{TaskID: 4, DependsOnTaskID: 3},
	}

	if HasCycle(tasks, dependencies) {
		t.Fatal("expected disconnected graph to be acyclic")
	}
}

func TestTopologicalOrderDiamondGraph(t *testing.T) {
	tasks := []Task{
		{ID: 1},
		{ID: 2},
		{ID: 3},
		{ID: 4},
	}

	dependencies := []Dependency{
		{TaskID: 2, DependsOnTaskID: 1},
		{TaskID: 3, DependsOnTaskID: 1},
		{TaskID: 4, DependsOnTaskID: 2},
		{TaskID: 4, DependsOnTaskID: 3},
	}

	order, err := TopologicalOrder(tasks, dependencies)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(order) != len(tasks) {
		t.Fatalf("expected %d tasks in order, got %d", len(tasks), len(order))
	}

	position := make(map[int64]int)

	for i, taskID := range order {
		position[taskID] = i
	}

	for _, dependency := range dependencies {
		if position[dependency.DependsOnTaskID] >= position[dependency.TaskID] {
			t.Fatalf(
				"dependency order violated: %d must come before %d",
				dependency.DependsOnTaskID,
				dependency.TaskID,
			)
		}
	}
}

func TestTopologicalOrderRejectsCycle(t *testing.T) {
	tasks := []Task{
		{ID: 1},
		{ID: 2},
		{ID: 3},
	}

	dependencies := []Dependency{
		{TaskID: 2, DependsOnTaskID: 1},
		{TaskID: 3, DependsOnTaskID: 2},
		{TaskID: 1, DependsOnTaskID: 3},
	}

	order, err := TopologicalOrder(tasks, dependencies)

	if !errors.Is(err, ErrCycleDetected) {
		t.Fatalf("expected ErrCycleDetected, got %v", err)
	}

	if order != nil {
		t.Fatalf("expected nil order for cyclic graph, got %v", order)
	}
}

func TestTopologicalOrderIsDeterministic(t *testing.T) {
	tasks := []Task{
		{ID: 4},
		{ID: 2},
		{ID: 1},
	}

	dependencies := []Dependency{
		{TaskID: 2, DependsOnTaskID: 1},
	}

	expected := []int64{1, 2, 4}

	order, err := TopologicalOrder(tasks, dependencies)
	if err != nil {
		t.Fatalf("TopologicalOrder() error = %v", err)
	}

	if !reflect.DeepEqual(order, expected) {
		t.Fatalf("TopologicalOrder() = %v, want %v", order, expected)
	}

	// Reverse the input order. The result should remain unchanged.
	reversedTasks := []Task{
		{ID: 1},
		{ID: 2},
		{ID: 4},
	}

	reversedDependencies := []Dependency{
		{TaskID: 2, DependsOnTaskID: 1},
	}

	reversedOrder, err := TopologicalOrder(
		reversedTasks,
		reversedDependencies,
	)
	if err != nil {
		t.Fatalf("TopologicalOrder() with reversed input error = %v", err)
	}

	if !reflect.DeepEqual(reversedOrder, expected) {
		t.Fatalf(
			"TopologicalOrder() with reversed input = %v, want %v",
			reversedOrder,
			expected,
		)
	}
}
