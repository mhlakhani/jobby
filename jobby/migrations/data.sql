-- INSERT INTO jobby_jobs (type, id, dependency, dependency_type, status, allowed_attempts, attempts) VALUES
--     (1, "one", null, null, 1, 2, 1),
--     (1, "two", "one", 1, 2, 2, 1),
--     (1, "three", "one", 1, 1, 2, 1),
--     (1, "four", "five", 1, 1, 2, 1),
--     (1, "six", "one", 1, 2, 2, 2),
--     (1, "seven", "one", 1, 3, 2, 2),
--     (1, "eight", "one", 1, 3, 2, 1),
--     (2, "two-ready", null, null, 1, 1, 0),
--     (2, "two-ready2", null, null, 1, 1, 0),
--     (3, "three-ready", null, null, 1, 1, 0),
--     (3, "three-ready2", null, null, 1, 1, 0),
--     (4, "four-ready", null, null, 1, 1, 0),
--     (5, "five-blocked", null, null, 2, 1, 0),
--     (6, "big-parent", null, null, 3, 1, 1),
--     (6, "first-dependency-completed", "big-parent", 6, 3, 1, 1),
--     (6, "second-dependency-waiting", "big-parent", 6, 2, 1, 1),
--     (6, "third-dependency-errored", "big-parent", 6, 4, 1, 1),
--     (6, "fourth-dependency-completed", "big-parent", 6, 3, 1, 1),
--     (6, "recursive-dependency-one", "first-dependency-completed", 6, 3, 1, 1),
--     (7, "recursive-dependency-two", "first-dependency-completed", 6, 3, 1, 1),
--     (8, "recursive-dependency-one-one", "recursive-dependency-one", 6, 3, 1, 1),
--     (9, "recursive-dependency-second", "second-dependency-waiting", 6, 3, 1, 1);

    -- 3 is completed, 1 is queued, 2 is waiting for dependency, 4 is errored

-- WITH C AS (
--     SELECT 
--         type, 
--         id, 
--         1 AS status 
--     FROM jobby_jobs
--     WHERE type = 6
--     AND   id = "big-parent"

--     UNION ALL

--     SELECT
--         S.type,
--         S.id,
--         2 AS status
--     FROM C AS M
--         INNER JOIN jobby_jobs AS S
--         ON S.dependency = M.id 
--         WHERE S.dependency_type = M.type
-- )
-- UPDATE jobby_jobs 
--     SET status = to_update.status,
--         attempts = 0
-- FROM (
--     SELECT
--         type, 
--         id,
--         status
--     FROM C
-- ) AS to_update
-- WHERE jobby_jobs.id = to_update.id
-- AND   jobby_jobs.type = to_update.type;

-- INSERT INTO jobby_workers (id, job_type, fetch_interval, status, last_fetch_time) VALUES 
--     -- ready workers for type 2
--     (1, 2, 5, 1, UNIXEPOCH()-10),
--     (2, 2, 5, 1, UNIXEPOCH()-10),
--     -- one ready and one blocked for type 3
--     (3, 3, 5, 1, UNIXEPOCH()-10),
--     (4, 3, 5, 2, UNIXEPOCH()-10),
--     -- one blocked and one still waiting for type 4
--     (5, 4, 5, 2, UNIXEPOCH()-10),
--     (6, 4, 5, 1, UNIXEPOCH()+3600),
--     -- one ready for type 5
--     (7, 5, 5, 1, UNIXEPOCH()-10);

-- WITH ranked_jobs_by_type AS (
--     SELECT 
--         job.id AS job_id,
--         job.job_type AS job_type,
--         job.ready_at AS ready_at,
--         row_number() OVER (
--             PARTITION BY job.job_type
--             ORDER BY job.id
--         ) job_rowid
--     FROM jobby_jobs job
--     WHERE job.status = 0
--     AND job.ready_at < UNIXEPOCH()
--     ORDER BY job.ready_at ASC
-- ), ranked_workers_by_type AS (
--     SELECT
--         worker.id AS worker_id,
--         worker.job_type as job_type,
--         row_number() OVER (
--             PARTITION BY worker.job_type
--             ORDER BY worker.id
--         ) worker_rowid
--     FROM jobby_workers worker
--     WHERE worker.status = 0
--     AND (
--             worker.last_fetch_time + 
--             worker.fetch_interval_ms / 1000 + (
--                 (ABS(RANDOM()) % (2*worker.fetch_interval_jitter_ms)) 
--                     - worker.fetch_interval_jitter_ms)/1000 
--         ) < UNIXEPOCH()
-- )
-- SELECT 
--     workers.worker_id,
--     jobs.job_id,
--     jobs.job_type
-- FROM 
--     ranked_jobs_by_type jobs
-- JOIN ranked_workers_by_type workers ON
--     jobs.job_type = workers.job_type
-- WHERE jobs.job_rowid = workers.worker_rowid;

-- SELECT
--     id,
--     job_type,
--     last_fetch_time,
--     next,
--     next < UNIXEPOCH() AS ready
-- FROM (
-- SELECT worker.id,
--     worker.job_type,
--     worker.last_fetch_time,
--     (worker.last_fetch_time + worker.fetch_interval_ms / 1000 + ((ABS(RANDOM()) % (2*worker.fetch_interval_jitter_ms)) - worker.fetch_interval_jitter_ms)/1000) AS next
-- FROM jobby_workers worker
-- );

-- WITH data(id, status) AS (
--     VALUES
--         ("two-ready", 5),
--         ("two-ready2", 5),
--         ("three-ready", 5)
-- )
-- UPDATE jobby_jobs
--     SET status = 5
-- FROM (
--     WITH data(id) AS (VALUES ("two_ready"), ("two-ready2"), ("three-ready"))
--     SELECT * FROM data
-- ) AS to_update
-- WHERE jobby_jobs.id = to_update.id;

-- UPDATE jobby_jobs
-- SET status = (
--     CASE 
--         WHEN attempts < allowed_attempts THEN 1
--         ELSE 4
--     END
-- ),
--     completion_time = UNIXEPOCH(),
--     error_message = "error"
-- WHERE type = 1
-- AND   id IN ("seven", "eight");

-- UPDATE jobby_jobs
-- SET status = 1
-- FROM (SELECT 
--     dep.type,
--     dep.id,
--     dep.status
-- FROM jobby_jobs dep
-- JOIN jobby_jobs parent 
-- ON dep.dependency = parent.id
-- WHERE dep.status = 2
-- AND parent.status = 1) AS to_update
-- WHERE jobby_jobs.id = to_update.id;


-- select id, job_type, dependency, dependency_type, status, ready_at from jobby_jobs;

-- SELECT 
--         dep.job_type,
--         dep.id,
--         dep.status
--     FROM jobby_jobs dep
--     JOIN jobby_jobs parent 
--     ON dep.dependency = parent.id
--     WHERE dep.status = 1
--     AND parent.status = 3
--     AND dep.dependency_type = parent.job_type;

-- WITH C AS (
--     SELECT 
--         job_type, 
--         id,
--         dependency_type
--     FROM jobby_jobs
--     WHERE job_type = 14
--     AND   id = "sentinel"

--     UNION ALL

--     SELECT
--         S.job_type,
--         S.id,
--         S.dependency_type
--     FROM C AS M
--         INNER JOIN jobby_jobs AS S
--         ON S.dependency = M.id 
--         WHERE S.dependency_type = M.job_type
-- )
-- UPDATE jobby_jobs 
--     SET status = (
--             CASE 
--                 WHEN to_update.dependency_type IS NULL THEN 0
--                 ELSE 1
--             END
--         ),
--         ready_at = UNIXEPOCH(),
--         attempts = 0
-- FROM (
--     SELECT
--         job_type, 
--         id,
--         dependency_type
--     FROM C
-- ) AS to_update
-- WHERE jobby_jobs.id = to_update.id
-- AND   jobby_jobs.job_type = to_update.job_type;


-- WITH root AS (
--     SELECT * FROM (
--         WITH data (job_type, id, ready_at) AS (
--             VALUES (19, "sentinel", 123)
--         )
--         SELECT * FROM data
--     )
-- ),
-- C AS (
--     SELECT 
--         jobby_jobs.job_type, 
--         jobby_jobs.id,
--         jobby_jobs.dependency,
--         jobby_jobs.dependency_type,
--         root.ready_at
--     FROM jobby_jobs
--     JOIN root ON root.id = jobby_jobs.id
--     WHERE root.id IS NOT NULL
--     AND root.job_type = jobby_jobs.job_type

--     UNION ALL

--     SELECT
--         S.job_type,
--         S.id,
--         S.dependency,
--         S.dependency_type,
--         M.ready_at
--     FROM C AS M
--         INNER JOIN jobby_jobs AS S
--         ON S.dependency = M.id 
--         WHERE S.dependency_type = M.job_type
-- )
-- SELECT
--     C.job_type, 
--     C.id,
--     C.dependency_type,
--     C.ready_at,
--     CASE
--         WHEN C.dependency_type IS NULL THEN 0
--         ELSE CASE
--             WHEN jobby_jobs.status = 3 THEN 
--                 CASE
--                     WHEN (
--                         (C.dependency IN (
--                             SELECT id FROM root WHERE job_type = C.dependency_type
--                         )) AND (C.dependency_type IN (
--                             SELECT job_type FROM root WHERE id = C.dependency
--                         ))
--                     ) THEN 1
--                     ELSE 0
--                 END
--             ELSE 1
--         END
--     END status 
-- FROM C
-- LEFT JOIN jobby_jobs
-- ON C.dependency = jobby_jobs.id
-- WHERE C.dependency_type IS NULL OR (C.dependency_type = jobby_jobs.job_type);



-- WITH root AS (
--     SELECT * FROM (
--         WITH data (job_type, id, ready_at) AS (
--             VALUES (19, "sentinel", 123)
--         )
--         SELECT * FROM data
--     )
-- ),
-- C AS (
--     SELECT
--         jobby_jobs.job_type,
--         jobby_jobs.id,
--         jobby_jobs.dependency,
--         jobby_jobs.dependency_type,
--         root.ready_at
--     FROM jobby_jobs
--     JOIN root ON root.id = jobby_jobs.id
--     WHERE root.id IS NOT NULL
--     AND root.job_type = jobby_jobs.job_type

--     UNION ALL

--     SELECT
--         S.job_type,
--         S.id,
--         S.dependency,
--         S.dependency_type,
--         M.ready_at
--     FROM C AS M
--         INNER JOIN jobby_jobs AS S
--         ON S.dependency = M.id
--         WHERE S.dependency_type = M.job_type
-- )
-- SELECT
--     C.job_type,
--     C.id,
--     C.dependency_type,
--     C.ready_at,
--     CASE
--         WHEN C.dependency_type IS NULL THEN 0
--         ELSE CASE
--             WHEN jobby_jobs.status = 3
--                 THEN
--                 CASE
--                     WHEN (
--                         (C.dependency IN (
--                             SELECT id FROM root WHERE job_type = C.dependency_type
--                         )) AND (C.dependency_type IN (
--                             SELECT job_type FROM root WHERE id = C.dependency
--                         ))
--                     ) THEN 1
--                     ELSE 0
--                 END
--             ELSE 1
--         END
--     END status
-- FROM C
-- LEFT JOIN jobby_jobs
-- ON C.dependency = jobby_jobs.id
-- WHERE C.dependency_type IS NULL OR (C.dependency_type = jobby_jobs.job_type);

-- UPDATE jobby_jobs 
--     SET status = (
--             CASE 
--                 WHEN to_update.dependency_type IS NULL THEN ?
--                 ELSE ?
--             END
--         ),
--         queue_time = ?,
--         ready_at = to_update.ready_at,
--         dequeue_time = NULL,
--         completion_time = NULL,
--         attempts = 0
-- FROM (
--     SELECT
--         job_type, 
--         id,
--         dependency_type,
--         ready_at
--     FROM C
-- ) AS to_update
-- WHERE jobby_jobs.id = to_update.id
-- AND   jobby_jobs.job_type = to_update.job_type;