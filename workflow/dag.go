package workflow

import (
	"container/heap"
	"errors"
)

var ErrCycleDetected = errors.New("workflow contains a cycle")

// HasCycle returns true when the dependency graph contains a cycle.
func HasCycle(tasks []Task, dependencies []Dependency) bool {
	inDegree := make(map[int64]int)
	graph := make(map[int64][]int64)

	for _, task := range tasks {
		inDegree[task.ID] = 0
	}

	for _, dependency := range dependencies {
		graph[dependency.DependsOnTaskID] =
			append(graph[dependency.DependsOnTaskID], dependency.TaskID)

		inDegree[dependency.TaskID]++
	}

	queue := make([]int64, 0)

	for _, task := range tasks {
		if inDegree[task.ID] == 0 {
			queue = append(queue, task.ID)
		}
	}

	processed := 0

	for len(queue) > 0 {
		taskID := queue[0]
		queue = queue[1:]

		processed++

		for _, dependentTaskID := range graph[taskID] {
			inDegree[dependentTaskID]--

			if inDegree[dependentTaskID] == 0 {
				queue = append(queue, dependentTaskID)
			}
		}
	}

	return processed != len(tasks)
}

func TopologicalOrder(tasks []Task, dependencies []Dependency) ([]int64, error) {
	inDegree := make(map[int64]int)
	graph := make(map[int64][]int64)

	for _, task := range tasks {
		inDegree[task.ID] = 0
	}

	for _, dependency := range dependencies {
		graph[dependency.DependsOnTaskID] =
			append(graph[dependency.DependsOnTaskID], dependency.TaskID)

		inDegree[dependency.TaskID]++
	}

	queue := &taskIDHeap{}
	heap.Init(queue)

	for _, task := range tasks {
		if inDegree[task.ID] == 0 {
			heap.Push(queue, task.ID)
		}
	}

	order := make([]int64, 0, len(tasks))

	for queue.Len() > 0 {
		taskID := heap.Pop(queue).(int64)
		order = append(order, taskID)

		for _, dependentTaskID := range graph[taskID] {
			inDegree[dependentTaskID]--

			if inDegree[dependentTaskID] == 0 {
				heap.Push(queue, dependentTaskID)
			}
		}
	}

	if len(order) != len(tasks) {
		return nil, ErrCycleDetected
	}

	return order, nil
}

type taskIDHeap []int64

func (h taskIDHeap) Len() int {
	return len(h)
}

func (h taskIDHeap) Less(i, j int) bool {
	return h[i] < h[j]
}

func (h taskIDHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *taskIDHeap) Push(value interface{}) {
	*h = append(*h, value.(int64))
}

func (h *taskIDHeap) Pop() interface{} {
	old := *h
	n := len(old)
	value := old[n-1]
	*h = old[:n-1]
	return value
}
