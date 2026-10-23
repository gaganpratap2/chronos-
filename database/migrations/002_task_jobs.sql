ALTER TABLE workflow_tasks
ADD COLUMN job_id BIGINT;

CREATE INDEX idx_workflow_tasks_job_id
ON workflow_tasks(job_id);