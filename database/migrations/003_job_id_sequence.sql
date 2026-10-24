CREATE SEQUENCE IF NOT EXISTS job_id_seq;

SELECT setval(
    'job_id_seq',
    COALESCE((SELECT MAX(id) FROM jobs), 0)
);