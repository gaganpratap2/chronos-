package workflow

// PropagateFailure marks tasks as Blocked when one of their dependencies
// has failed or become blocked.
func PropagateFailure(
	tasks []Task, dependencies []Dependency,
) []Task {

	taskByID := make(map[int64]int)

	for i := range tasks {
		taskByID[tasks[i].ID] = i
	}

	changed := true

	for changed {
		changed = false

		for _, dependency := range dependencies {
			taskidx, taskExists := taskByID[dependency.TaskID]
			dependencyIndex, dependencyExists := taskByID[dependency.DependsOnTaskID]

			if !taskExists || !dependencyExists {
				continue
			}

			dependencyStatus := tasks[dependencyIndex].Status
			taskStatus := tasks[taskidx].Status

			if (dependencyStatus == TaskFailed ||
				dependencyStatus == TaskBlocked) &&
				taskStatus == TaskPending {

				tasks[taskidx].Status = TaskBlocked
				changed = true
			}
		}
	}

	return tasks
}
