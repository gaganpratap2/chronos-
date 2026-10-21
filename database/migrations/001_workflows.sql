CREATE TABLE workflows (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE workflow_tasks (
    id BIGSERIAL PRIMARY KEY,
    workflow_id BIGINT NOT NULL
        REFERENCES workflows(id)
        ON DELETE CASCADE,
    name TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE task_dependencies (
    id BIGSERIAL PRIMARY KEY,
    task_id BIGINT NOT NULL
        REFERENCES workflow_tasks(id)
        ON DELETE CASCADE,
    depends_on_task_id BIGINT NOT NULL
        REFERENCES workflow_tasks(id)
        ON DELETE CASCADE,

    CONSTRAINT unique_task_dependency
        UNIQUE (task_id, depends_on_task_id),

    CONSTRAINT no_self_dependency
        CHECK (task_id <> depends_on_task_id)
);

CREATE INDEX idx_workflow_tasks_workflow_id
    ON workflow_tasks(workflow_id);

CREATE INDEX idx_task_dependencies_task_id
    ON task_dependencies(task_id);

CREATE INDEX idx_task_dependencies_depends_on_task_id
    ON task_dependencies(depends_on_task_id);