-- optimize fetching for new query
CREATE INDEX jobby_jobs_job_type_status_ready_at ON jobby_jobs(job_type, status, ready_at);