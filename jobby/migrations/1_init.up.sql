CREATE TABLE jobby_blob_store (
    job_type INTEGER NOT NULL,
    id TEXT NOT NULL,
    is_input INTEGER NOT NULL,
    contents BLOB,
    metadata TEXT NOT NULL,
    updated_time INTEGER NOT NULL,
    expires_at INTEGER,
    PRIMARY KEY(job_type, id, is_input)
);
CREATE INDEX jobby_blob_store_job_type ON jobby_blob_store(job_type);
CREATE INDEX jobby_blob_store_id ON jobby_blob_store(id);
CREATE INDEX jobby_blob_store_job_type_id ON jobby_blob_store(job_type, id);
CREATE INDEX jobby_blob_store_job_type_id_is_input ON jobby_blob_store(job_type, id, is_input);
CREATE INDEX jobby_blob_store_expires_at ON jobby_blob_store(expires_at);

CREATE TABLE jobby_jobs (
    job_type INTEGER NOT NULL,
    id TEXT NOT NULL,
    dependency TEXT,
    dependency_type INTEGER,
    status INTEGER NOT NULL,
    queue_time INTEGER NOT NULL,
    ready_at INTEGER NOT NULL,
    dequeue_time INTEGER,
    completion_time INTEGER,
    allowed_attempts INTEGER NOT NULL,
    attempts INTEGER NOT NULL,
    error_message TEXT,
    -- how long after success should the job expire
    expiry INTEGER,
    expires_at INTEGER,
    PRIMARY KEY(job_type, id)
);
CREATE INDEX jobby_jobs_job_type ON jobby_jobs(job_type);
CREATE INDEX jobby_jobs_id ON jobby_jobs(id);
CREATE INDEX jobby_jobs_job_type_id ON jobby_jobs(job_type, id);
CREATE INDEX jobby_jobs_status ON jobby_jobs(status);
CREATE INDEX jobby_jobs_queue_time ON jobby_jobs(queue_time);
CREATE INDEX jobby_jobs_ready_at ON jobby_jobs(ready_at);
CREATE INDEX jobby_jobs_dependency ON jobby_jobs(dependency);
CREATE INDEX jobby_jobs_expires_at ON jobby_jobs(expires_at);
CREATE INDEX jobby_jobs_completion_time ON jobby_jobs(completion_time);

CREATE TABLE jobby_workers (
    id TEXT PRIMARY KEY NOT NULL,
    job_type INTEGER NOT NULL,
    fetch_interval INTEGER,
    fetch_interval_jitter INTEGER,
    status INTEGER NOT NULL,
    last_fetch_time INTEGER NOT NULL
);
CREATE INDEX jobby_workers_fetching ON jobby_workers(job_type, status, last_fetch_time);