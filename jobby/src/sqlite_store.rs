use crate::{
    action::Actions,
    blob_encoding::BlobMetadata,
    db::JobbyDb,
    states::{
        BlobIsInputOrOutput, JobAssignment, JobFullState, JobFullStateWithData, JobState,
        JobStateAndWorkerId, JobStateForDAG, JobStatus, JobTypeAndWorkerId, UnnamespacedJobType,
        UnnamespacedWorkerSpec, WorkerId, WorkerStatus,
    },
    store::{BackfillableJob, HandleJobCompletionStatus, JobbyStore, SubmittableJob},
    Error, JobBuilder, JobId, JobbyBlob,
};

use std::{
    collections::HashMap,
    time::{Duration, SystemTime},
};

use futures::StreamExt;
use itertools::Itertools;
use rusqlite::{Connection, Transaction};

use rocket_sqlite_rw_pool::{
    execute_with_params, query_optional_with_params, query_with_params, BatchedBulkValuesClause,
    ConnectionPool, WriteAuthorization,
};

type Result<T, E = crate::Error> = anyhow::Result<T, E>;
type Write = (
    UnnamespacedJobType,
    JobId,
    BlobIsInputOrOutput,
    BlobMetadata,
    Vec<u8>,
);

const INSERT_BATCH_SIZE: usize = 2000;

pub struct SqliteJobbyStore(pub ConnectionPool<JobbyDb>);

impl SqliteJobbyStore {
    // TODO move to common place
    fn status_from_dependency(dependency: &Option<UnnamespacedJobType>) -> JobStatus {
        if dependency.is_some() {
            JobStatus::WaitingForDependency
        } else {
            JobStatus::Queued
        }
    }

    // TODO Move to common place
    fn now() -> i64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0))
            .as_millis() as i64
    }

    fn free_worker(
        transaction: &Transaction,
        job_type: UnnamespacedJobType,
        id: WorkerId,
    ) -> Result<()> {
        execute_with_params(
            r"
UPDATE jobby_workers
SET status = ?
WHERE job_type = ?
AND   id = ?
AND   status = ?",
            transaction,
            &(
                WorkerStatus::WaitingForJob,
                job_type,
                id.clone(),
                WorkerStatus::RunningJob,
            ),
        )
        .map_err(|e| Error::WorkerFreeing(id, e))?;
        Ok(())
    }

    fn bulk_update_job_data(
        transaction: &Transaction,
        rows: Vec<Write>,
        // if set, uses REPLACE INTO instead of INSERT OR IGNORE INTO
        replace: bool,
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let preamble = if replace {
            "REPLACE"
        } else {
            "INSERT OR IGNORE"
        };

        let now = Self::now();
        let row_count = rows.len();
        let rows = rows
            .into_iter()
            .filter_map(|(job_type, job_id, is_input, metadata, data)| {
                serde_json::to_string(&metadata).ok().map(|encoded| {
                    (
                        job_type,
                        job_id,
                        is_input,
                        encoded,
                        data,
                        now,
                        None::<Option<i64>>,
                    )
                })
            });

        let insert = BatchedBulkValuesClause::new(Box::new(move |clause| {
            format!(
                r#"
{} INTO jobby_blob_store (
    job_type, 
    id, 
    is_input,
    metadata, 
    contents,
    updated_time,
    expires_at
) 
{clause}
;"#,
                &preamble
            )
        }));
        insert
            .execute(transaction, row_count, INSERT_BATCH_SIZE, rows)
            .map_err(Error::BlobInsertion)?;
        Ok(())
    }

    fn mark_job_succeeded(
        transaction: &Transaction,
        job_type: UnnamespacedJobType,
        job_id: JobId,
        worker_id: WorkerId,
    ) -> Result<()> {
        execute_with_params(
            r"
UPDATE jobby_jobs
SET status = ?,
    completion_time = ?,
    error_message = NULL,
    expires_at = (
        CASE
            WHEN expiry IS NULL THEN NULL
            ELSE expiry + ?
        END
    )
WHERE job_type = ?
AND   id = ?
AND   status = ?",
            transaction,
            &(
                JobStatus::Succeeded,
                Self::now(),
                Self::now(),
                job_type,
                job_id.clone(),
                JobStatus::Dequeued,
            ),
        )
        .map_err(|e| Error::JobSuccessMarking(job_id.clone(), e))?;
        execute_with_params(
            r"
UPDATE jobby_blob_store
SET 
    expires_at = to_update.expires_at
FROM (
    SELECT 
        job_type,
        id,
        expires_at
    FROM jobby_jobs
    WHERE job_type = ?
    AND id = ?
) AS to_update
WHERE jobby_blob_store.id = to_update.id
AND   jobby_blob_store.job_type = to_update.job_type;",
            transaction,
            &(job_type, job_id.clone()),
        )
        .map_err(|e| Error::JobSuccessMarking(job_id.clone(), e))?;
        Self::free_worker(transaction, job_type, worker_id)?;
        execute_with_params(
            r"
UPDATE jobby_jobs
SET 
    status = ?
WHERE jobby_jobs.status = ?
AND   jobby_jobs.dependency_type = ?
AND   jobby_jobs.dependency = ?;",
            transaction,
            &(
                JobStatus::Queued,
                JobStatus::WaitingForDependency,
                job_type,
                job_id.clone(),
            ),
        )
        .map_err(|e| Error::JobDependencyResolving(job_id, e))?;
        Ok(())
    }

    fn mark_job_failed(
        transaction: &Transaction,
        job_type: UnnamespacedJobType,
        job_id: JobId,
        worker_id: WorkerId,
        error_message: String,
    ) -> Result<()> {
        execute_with_params(
            r"
UPDATE jobby_jobs
SET status = (
    CASE 
        WHEN attempts < allowed_attempts THEN ?
        ELSE ?
    END
),
    completion_time = ?,
    error_message = ?
WHERE job_type = ?
AND   id = ?
AND   status = ?",
            transaction,
            &(
                JobStatus::Queued,
                JobStatus::Failed,
                Self::now(),
                error_message,
                job_type,
                job_id.clone(),
                JobStatus::Dequeued,
            ),
        )
        .map_err(|e| Error::JobFailureMarking(job_id, e))?;
        Self::free_worker(transaction, job_type, worker_id)?;
        Ok(())
    }

    async fn encode_write(data: JobbyBlob) -> Result<(BlobMetadata, Vec<u8>)> {
        Ok(data.encode().await?.into_parts())
    }

    // TODO: Somehow make common
    async fn process_actions(
        job_type: UnnamespacedJobType,
        job_id: JobId,
        actions: Actions,
    ) -> Result<(Vec<BackfillableJob>, Vec<SubmittableJob>, Vec<Write>)> {
        let mut updates: Vec<Write> = vec![];

        // Encode the writes in bulk
        if !actions.writes.is_empty() {
            let futures = actions.writes.into_iter().map(Self::encode_write);
            let stream = futures::stream::iter(futures).buffered(crate::FUTURES_IN_PARALLEL);
            let maybe_metadata_and_data = stream.collect::<Vec<_>>().await;
            let metadata_and_data = maybe_metadata_and_data
                .into_iter()
                .collect::<Result<Vec<_>>>()?;
            updates.extend(metadata_and_data.into_iter().map(|(metadata, data)| {
                (
                    job_type,
                    job_id.clone(),
                    BlobIsInputOrOutput::Output,
                    metadata,
                    data,
                )
            }));
        }

        // Create the jobs in bulk too as it does encoding
        let (backfills, submits) = JobBuilder::bulk_into_jobs(actions.jobs).await?;

        Ok((backfills, submits, updates))
    }

    fn submit_jobs_inner(transaction: &Transaction, mut jobs: Vec<SubmittableJob>) -> Result<()> {
        if jobs.is_empty() {
            return Ok(());
        }

        // Insert blobs
        let blobs: Vec<_> = jobs
            .iter_mut()
            .map(|job| {
                let metadata = std::mem::take(&mut job.data.0);
                let data = std::mem::take(&mut job.data.1);
                (
                    job.job_type,
                    job.id.clone(),
                    BlobIsInputOrOutput::Input,
                    metadata,
                    data,
                )
            })
            .collect();
        Self::bulk_update_job_data(transaction, blobs, false)?;

        // Insert jobs
        let now = Self::now();
        let row_count = jobs.len();
        let rows = jobs.into_iter().map(|job| {
            (
                job.job_type,
                job.id,
                job.dependency,
                job.dependency_type,
                Self::status_from_dependency(&job.dependency_type),
                now,
                now,
                job.allowed_attempts,
                0_i32, // attempts
                job.expiry.map(|expiry| expiry.as_millis() as i64),
                None::<Option<i64>>,
            )
        });

        let insert = BatchedBulkValuesClause::new(Box::new(|clause| {
            format!(
                r#"
INSERT OR IGNORE INTO jobby_jobs (
    job_type,
    id,
    dependency,
    dependency_type,
    status,
    queue_time,
    ready_at,
    allowed_attempts,
    attempts,
    expiry,
    expires_at
) 
{clause}
;"#
            )
        }));
        insert
            .execute(transaction, row_count, INSERT_BATCH_SIZE, rows)
            .map_err(Error::JobInsertion)?;
        Ok(())
    }

    fn backfill_jobs_inner(
        transaction: &Transaction,
        mut jobs: Vec<BackfillableJob>,
    ) -> Result<()> {
        if jobs.is_empty() {
            return Ok(());
        }
        // Insert blobs
        let blobs: Vec<_> = jobs
            .iter_mut()
            .filter_map(|job| {
                job.data.take().map(|(metadata, data)| {
                    (
                        job.job_type,
                        job.id.clone(),
                        BlobIsInputOrOutput::Input,
                        metadata,
                        data,
                    )
                })
            })
            .collect();
        Self::bulk_update_job_data(transaction, blobs, true)?;

        // Backfill jobs
        let row_count = jobs.len();
        let rows = jobs.into_iter().map(|job| {
            (
                job.job_type,
                job.id,
                job.ready_at
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or(Duration::from_secs(0))
                    .as_millis() as i64,
            )
        });

        let mut insert = BatchedBulkValuesClause::new(Box::new(move |clause| {
            format!(
                r#"
WITH root AS (
    SELECT * FROM (
        WITH data (job_type, id, ready_at) AS (
            {clause}
        )
        SELECT * FROM data
    )
),
C AS (
    SELECT 
        jobby_jobs.job_type, 
        jobby_jobs.id,
        jobby_jobs.dependency,
        jobby_jobs.dependency_type,
        root.ready_at
    FROM jobby_jobs
    JOIN root ON root.id = jobby_jobs.id
    WHERE root.id IS NOT NULL
    AND root.job_type = jobby_jobs.job_type

    UNION ALL

    SELECT
        S.job_type,
        S.id,
        S.dependency,
        S.dependency_type,
        M.ready_at
    FROM C AS M
        INNER JOIN jobby_jobs AS S
        ON S.dependency = M.id 
        WHERE S.dependency_type = M.job_type
)
UPDATE jobby_jobs 
    SET status = to_update.status,
        queue_time = ?,
        ready_at = to_update.ready_at,
        dequeue_time = NULL,
        completion_time = NULL,
        attempts = 0,
        expires_at = NULL
FROM (
    SELECT
        C.job_type, 
        C.id,
        C.dependency_type,
        C.ready_at,
        CASE
            WHEN C.dependency_type IS NULL THEN ?
            ELSE ?
        END status 
    FROM C
    LEFT JOIN jobby_jobs
    ON C.dependency = jobby_jobs.id
    WHERE C.dependency_type IS NULL OR (C.dependency_type = jobby_jobs.job_type)
) AS to_update
WHERE jobby_jobs.id = to_update.id
AND   jobby_jobs.job_type = to_update.job_type;"#
            )
        }));
        insert
            .bind_post(&(
                Self::now(),
                JobStatus::Queued,
                JobStatus::WaitingForDependency,
            ))
            .map_err(Error::JobBackfilling)?;
        insert
            .execute(transaction, row_count, INSERT_BATCH_SIZE, rows)
            .map_err(Error::JobBackfilling)?;
        Ok(())
    }

    fn fetch_and_claim_jobs_inner(transaction: &Transaction) -> Result<Vec<JobStateAndWorkerId>> {
        let now = Self::now();
        // This query is specifically crafted to make it O(ready workers) and not O(queued jobs)
        // to optimize CPU usage.
        // Note: We *assume* only one worker exists per job type, or that at most
        // one job of a job type will be dispatched per cycle (can be many workers)
        // TODO: Enforce or document this assumption
        // We also pick *any* ready job, not necessarily the oldest one (though in practice it is.)
        // We first pull out ready workers and then pull out ready jobs (again one per type)
        // which have the job type IN the list.
        // The IN clause was the fastest of all the methods tested (see diff for explanation).
        // Here's the query plan:
        // QUERY PLAN
        // |--MATERIALIZE ranked_via_workers
        // |  |--SEARCH job USING INDEX jobby_jobs_job_type_status_ready_at (job_type=? AND status=? AND ready_at<?)
        // |  `--LIST SUBQUERY 2
        // |     |--MATERIALIZE ranked_workers_by_type
        // |     |  `--SCAN worker USING INDEX jobby_workers_fetching
        // |     `--SCAN ranked_workers_by_type
        // |--SCAN ranked
        // `--SEARCH workers USING AUTOMATIC COVERING INDEX (job_type=?)
        let assignments: Vec<JobAssignment> = query_with_params(
            r"
WITH ranked_workers_by_type AS (
    SELECT
        worker.id AS worker_id,
        worker.job_type as job_type
    FROM jobby_workers worker
    WHERE worker.status = ?
    AND (
            worker.last_fetch_time + 
            worker.fetch_interval + (
                (ABS(RANDOM()) % (2*worker.fetch_interval_jitter)) 
                    - worker.fetch_interval_jitter)
        ) < ?
    GROUP BY worker.job_type
),
ranked_via_workers AS (
    SELECT 
        job.id AS job_id,
        job.job_type AS job_type
    FROM jobby_jobs job
    WHERE job.status = ?
    AND job.ready_at < ?
    AND job.job_type IN (SELECT job_type FROM ranked_workers_by_type)
    GROUP BY job.job_type
)
SELECT 
    workers.worker_id,
    ranked.job_id,
    ranked.job_type
FROM ranked_via_workers ranked
JOIN ranked_workers_by_type workers
ON   ranked.job_type = workers.job_type;",
            transaction,
            &(WorkerStatus::WaitingForJob, now, JobStatus::Queued, now),
        )
        .map_err(Error::JobFetchingAndClaiming)?;

        // TODO: Trace
        if assignments.is_empty() {
            return Ok(vec![]);
        }

        let job_states: Vec<JobState> = {
            let mut insert = BatchedBulkValuesClause::new(Box::new(move |clause| {
                format!(
                    r#"
UPDATE jobby_jobs
SET status = ?,
    dequeue_time = ?,
    attempts = attempts + 1
FROM (
WITH data(id, job_type) AS (
    {clause}
)
SELECT
    *
FROM data
) AS to_update
WHERE jobby_jobs.job_type = to_update.job_type
AND jobby_jobs.id = to_update.id
RETURNING *;"#
                )
            }));
            insert
                .bind_pre(&(JobStatus::Dequeued, now))
                .map_err(Error::JobFetchingAndClaiming)?;

            let row_count = assignments.len();
            let rows = assignments
                .iter()
                .map(|assignment| (assignment.job_id.clone(), assignment.job_type));
            insert
                .query(transaction, row_count, INSERT_BATCH_SIZE, rows)
                .map_err(Error::JobFetchingAndClaiming)?
        };

        // TODO: Trace
        let mut worker_ids: HashMap<UnnamespacedJobType, Vec<WorkerId>> = {
            let mut insert = BatchedBulkValuesClause::new(Box::new(move |clause| {
                format!(
                    r#"
UPDATE jobby_workers
SET status = ?,
    last_fetch_time = ?
FROM (
WITH data(id) AS (
    {clause}
)
SELECT
    *
FROM data
) AS to_update
WHERE jobby_workers.id = to_update.id
RETURNING
jobby_workers.job_type AS job_type,
jobby_workers.id AS worker_id;"#
                )
            }));
            insert
                .bind_pre(&(WorkerStatus::RunningJob, now))
                .map_err(Error::JobFetchingAndClaiming)?;

            let row_count = assignments.len();
            let rows = assignments
                .iter()
                .map(|assignment| assignment.worker_id.clone());

            let rows: Vec<JobTypeAndWorkerId> = insert
                .query(transaction, row_count, INSERT_BATCH_SIZE, rows)
                .map_err(Error::JobFetchingAndClaiming)?;
            rows.into_iter()
                .sorted_by(|a, b| Ord::cmp(&a.job_type, &b.job_type))
                .group_by(|t| t.job_type)
                .into_iter()
                .map(|(key, group)| (key, group.into_iter().map(|r| r.worker_id).collect()))
                .collect()
        };
        // TODO: Trace

        let workers_count = worker_ids.values().map(std::vec::Vec::len).sum::<usize>();
        if workers_count != job_states.len() {
            return Err(Error::JobFetchingAndClaimingMismatchedLengths(
                workers_count,
                job_states.len(),
            ));
        }

        let maybe_result: Result<Vec<JobStateAndWorkerId>> = job_states
            .into_iter()
            .map(|job| {
                let worker_id = worker_ids
                    .get_mut(&job.job_type)
                    .ok_or_else(|| {
                        Error::JobFetchingAndClaimingMismatchedJobType(job.job_type.id())
                    })?
                    .pop()
                    .ok_or_else(|| {
                        Error::JobFetchingAndClaimingMismatchedJobType(job.job_type.id())
                    })?;
                Ok(JobStateAndWorkerId { job, worker_id })
            })
            .collect();

        let result = maybe_result?;
        Ok(result)
    }

    fn register_workers_inner(
        transaction: &Transaction,
        workers: Vec<UnnamespacedWorkerSpec>,
    ) -> Result<()> {
        if workers.is_empty() {
            return Ok(());
        }

        let insert = BatchedBulkValuesClause::new(Box::new(move |clause| {
            format!(
                r#"
INSERT INTO jobby_workers (
    id,
    job_type,
    fetch_interval,
    fetch_interval_jitter,
    status,
    last_fetch_time
) 
{clause}
;"#
            )
        }));
        let row_count = workers.len();
        let now = Self::now();
        let rows = workers.into_iter().map(|worker| {
            let last_fetch_time = if let Some(time) = worker.last_fetch_time {
                time.duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or(Duration::from_secs(0))
                    .as_millis() as i64
            } else {
                now + (worker.fetch_interval.as_millis() as i64)
            };
            (
                worker.id,
                worker.job_type,
                worker.fetch_interval.as_millis() as i64,
                worker.fetch_jitter.as_millis() as i64,
                WorkerStatus::WaitingForJob,
                last_fetch_time,
            )
        });
        insert
            .execute(transaction, row_count, INSERT_BATCH_SIZE, rows)
            .map_err(Error::WorkerRegistration)?;
        Ok(())
    }

    // Fetch the root if one exists
    fn fetch_dag_root(
        connection: &Connection,
        job_type: UnnamespacedJobType,
        job_id: &JobId,
    ) -> Result<Option<JobStateForDAG>> {
        query_optional_with_params(
            r"
WITH leaf (job_type, id) AS (
    VALUES (?, ?)
),
C AS (
    SELECT 
        jobby_jobs.job_type, 
        jobby_jobs.id,
        jobby_jobs.dependency,
        jobby_jobs.dependency_type,
        jobby_jobs.status
    FROM jobby_jobs
    JOIN leaf ON leaf.id = jobby_jobs.id
    WHERE leaf.id IS NOT NULL
    AND leaf.job_type = jobby_jobs.job_type

    UNION ALL

    SELECT
        S.job_type,
        S.id,
        S.dependency,
        S.dependency_type,
        M.status
    FROM C AS M
        INNER JOIN jobby_jobs AS S
        ON S.id = M.dependency
        WHERE S.job_type = M.dependency_type
)
SELECT *
FROM C
WHERE C.dependency IS NULL
;",
            connection,
            &(job_type.id(), job_id),
        )
        .map_err(Error::JobDagFetching)
    }

    fn fetch_dag_inner(
        connection: &Connection,
        job_type: UnnamespacedJobType,
        job_id: JobId,
    ) -> Result<Vec<JobStateForDAG>> {
        // Start from the root if we can - if we can't due to expiry, then use the current node
        let root = Self::fetch_dag_root(connection, job_type, &job_id)?;
        let (job_type, id) = match root {
            Some(row) => (row.job_type, row.id),
            None => (job_type.id(), job_id),
        };
        query_with_params(
            r"
WITH root (job_type, id) AS (
    VALUES (?, ?)
),
C AS (
    SELECT 
        jobby_jobs.job_type, 
        jobby_jobs.id,
        jobby_jobs.dependency,
        jobby_jobs.dependency_type,
        jobby_jobs.status
    FROM jobby_jobs
    JOIN root ON root.id = jobby_jobs.id
    WHERE root.id IS NOT NULL
    AND root.job_type = jobby_jobs.job_type

    UNION ALL

    SELECT
        S.job_type,
        S.id,
        S.dependency,
        S.dependency_type,
        S.status
    FROM C AS M
        INNER JOIN jobby_jobs AS S
        ON S.dependency = M.id 
        WHERE S.dependency_type = M.job_type
)
SELECT *
FROM C
;",
            connection,
            &(job_type, id),
        )
        .map_err(Error::JobDagFetching)
    }
}

#[derive(Debug)]
enum HandleJobCompletionHelper {
    Failure(String),
    Success((Vec<BackfillableJob>, Vec<SubmittableJob>, Vec<Write>)),
}

#[async_trait::async_trait]
impl JobbyStore for SqliteJobbyStore {
    async fn handle_job_completion(
        &self,
        job_type: UnnamespacedJobType,
        job_id: JobId,
        worker_id: WorkerId,
        status: HandleJobCompletionStatus,
    ) -> Result<()> {
        // Do the heavy processing before heading into the transaction or getting a connection
        let helper = match status {
            HandleJobCompletionStatus::Failure(error_message) => {
                HandleJobCompletionHelper::Failure(error_message)
            }
            HandleJobCompletionStatus::Success(actions) => {
                let writes = Self::process_actions(job_type, job_id.clone(), actions).await?;
                HandleJobCompletionHelper::Success(writes)
            }
        };
        self.0
            .connect_and_write(
                WriteAuthorization::IPromiseThisIsABackgroundJobNotTiedToARequest,
                move |transaction| -> Result<()> {
                    match helper {
                        HandleJobCompletionHelper::Failure(error_message) => {
                            Self::mark_job_failed(
                                &transaction,
                                job_type,
                                job_id.clone(),
                                worker_id,
                                error_message,
                            )?;
                            transaction
                                .commit()
                                .map_err(|e| Error::JobFailureMarking(job_id, e))?;
                        }
                        HandleJobCompletionHelper::Success((backfills, submits, updates)) => {
                            if !updates.is_empty() {
                                Self::bulk_update_job_data(&transaction, updates, true)?;
                            }
                            if !submits.is_empty() {
                                Self::submit_jobs_inner(&transaction, submits)?;
                            }
                            if !backfills.is_empty() {
                                Self::backfill_jobs_inner(&transaction, backfills)?;
                            }
                            Self::mark_job_succeeded(
                                &transaction,
                                job_type,
                                job_id.clone(),
                                worker_id,
                            )?;
                            transaction
                                .commit()
                                .map_err(|e| Error::JobSuccessMarking(job_id, e))?;
                        }
                    }
                    Ok(())
                },
            )
            .await
            .map_err(Error::ConnectionFetching)??;
        Ok(())
    }

    async fn submit_jobs(&self, jobs: Vec<SubmittableJob>) -> Result<()> {
        self.0
            .connect_and_write(
                WriteAuthorization::IPromiseThisIsABackgroundJobNotTiedToARequest,
                move |transaction| -> Result<()> {
                    Self::submit_jobs_inner(&transaction, jobs)?;
                    transaction.commit().map_err(Error::JobInsertion)?;
                    Ok(())
                },
            )
            .await
            .map_err(Error::ConnectionFetching)??;
        Ok(())
    }

    async fn fetch_and_claim_jobs(&self) -> Result<Vec<JobStateAndWorkerId>> {
        let rows = self
            .0
            .connect_and_write(
                WriteAuthorization::IPromiseThisIsABackgroundJobNotTiedToARequest,
                move |transaction| -> Result<Vec<JobStateAndWorkerId>> {
                    let rows = Self::fetch_and_claim_jobs_inner(&transaction)?;
                    transaction
                        .commit()
                        .map_err(Error::JobFetchingAndClaiming)?;
                    Ok(rows)
                },
            )
            .await
            .map_err(Error::ConnectionFetching)??;
        Ok(rows)
    }

    async fn fetch_job_data(
        &self,
        job_type: UnnamespacedJobType,
        id: JobId,
        input_or_output: BlobIsInputOrOutput,
    ) -> Result<Option<(BlobMetadata, Vec<u8>, i64)>> {
        let data = self
            .0
            .connect_and_read(
                move |connection| -> Result<Option<(BlobMetadata, Vec<u8>, i64)>> {
                    let result: Option<(String, Vec<u8>, i64)> = query_optional_with_params(
                        r"
SELECT metadata, contents, updated_time
FROM jobby_blob_store
WHERE job_type = ?
AND id = ?
AND is_input = ?
LIMIT 1;",
                        connection,
                        &(job_type, id.clone(), input_or_output),
                    )
                    .map_err(|e| Error::BlobFetching(id.clone(), e))?;
                    if let Some((encoded, contents, updated_time)) = result {
                        let metadata: BlobMetadata = serde_json::from_str(&encoded)?;
                        Ok(Some((metadata, contents, updated_time)))
                    } else {
                        Ok(None)
                    }
                },
            )
            .await
            .map_err(Error::ConnectionFetching)??;
        Ok(data)
    }

    async fn startup(&self) -> Result<()> {
        self.0
            .connect_and_write(
                WriteAuthorization::IPromiseThisIsABackgroundJobNotTiedToARequest,
                move |transaction| -> Result<()> {
                    // don't bother preparing, this is used once
                    transaction
                        .execute("DELETE FROM jobby_workers;", [])
                        .map_err(Error::Startup)?;
                    // Set all the dequeued jobs back to queued
                    // Don't need to worry about dependencies - it can only be dequeued
                    // if it was queued already and that happens after dependencies are OK
                    execute_with_params(
                        r"
UPDATE jobby_jobs
SET status = ?,
    dequeue_time = NULL,
    attempts = attempts - 1
WHERE status = ?",
                        &transaction,
                        &(JobStatus::Queued, JobStatus::Dequeued),
                    )
                    .map_err(Error::Startup)?;
                    transaction.commit().map_err(Error::Startup)?;
                    Ok(())
                },
            )
            .await
            .map_err(Error::ConnectionFetching)??;
        Ok(())
    }

    async fn register_workers(&self, workers: Vec<UnnamespacedWorkerSpec>) -> Result<()> {
        self.0
            .connect_and_write(
                WriteAuthorization::IPromiseThisIsABackgroundJobNotTiedToARequest,
                move |transaction| -> Result<()> {
                    Self::register_workers_inner(&transaction, workers)?;
                    transaction.commit().map_err(Error::WorkerRegistration)?;
                    Ok(())
                },
            )
            .await
            .map_err(Error::ConnectionFetching)??;
        Ok(())
    }

    async fn backfill_jobs(&self, jobs: Vec<BackfillableJob>) -> Result<()> {
        self.0
            .connect_and_write(
                WriteAuthorization::IPromiseThisIsABackgroundJobNotTiedToARequest,
                move |transaction| -> Result<()> {
                    Self::backfill_jobs_inner(&transaction, jobs)?;
                    transaction.commit().map_err(Error::JobBackfilling)?;
                    Ok(())
                },
            )
            .await
            .map_err(Error::ConnectionFetching)??;
        Ok(())
    }

    async fn expire_jobs(&self) -> Result<()> {
        let now = Self::now();
        self.0
            .connect_and_write(
                WriteAuthorization::IPromiseThisIsABackgroundJobNotTiedToARequest,
                move |transaction| -> Result<()> {
                    execute_with_params(
                        r"
DELETE FROM jobby_jobs
WHERE expires_at IS NOT NULL AND expires_at < ?
;",
                        &transaction,
                        &(now),
                    )
                    .map_err(Error::Expiry)?;
                    execute_with_params(
                        r"
DELETE FROM jobby_blob_store
WHERE expires_at IS NOT NULL AND expires_at < ?
;",
                        &transaction,
                        &(now),
                    )
                    .map_err(Error::Expiry)?;
                    transaction.commit().map_err(Error::Expiry)?;
                    Ok(())
                },
            )
            .await
            .map_err(Error::ConnectionFetching)??;
        Ok(())
    }

    async fn expire_job(
        &self,
        job_type: UnnamespacedJobType,
        job_id: JobId,
        duration: Duration,
    ) -> Result<()> {
        let expires_at = Self::now() + (duration.as_millis() as i64);
        self.0
            .connect_and_write(
                WriteAuthorization::IPromiseThisIsABackgroundJobNotTiedToARequest,
                move |transaction| -> Result<()> {
                    execute_with_params(
                        r"
UPDATE jobby_jobs
SET expires_at = ?
WHERE job_type = ?
AND id = ?
;",
                        &transaction,
                        &(expires_at, job_type.id(), job_id),
                    )
                    .map_err(Error::Expiry)?;
                    transaction.commit().map_err(Error::Expiry)?;
                    Ok(())
                },
            )
            .await
            .map_err(Error::ConnectionFetching)??;
        Ok(())
    }

    async fn unexpire_job(&self, job_type: UnnamespacedJobType, job_id: JobId) -> Result<()> {
        self.0
            .connect_and_write(
                WriteAuthorization::IPromiseThisIsABackgroundJobNotTiedToARequest,
                move |transaction| -> Result<()> {
                    execute_with_params(
                        r"
UPDATE jobby_jobs
SET expires_at = NULL
WHERE job_type = ?
AND id = ?
;",
                        &transaction,
                        &(job_type.id(), job_id),
                    )
                    .map_err(Error::Expiry)?;
                    transaction.commit().map_err(Error::Expiry)?;
                    Ok(())
                },
            )
            .await
            .map_err(Error::ConnectionFetching)??;
        Ok(())
    }

    async fn job_status_metrics(
        &self,
        job_type: Option<UnnamespacedJobType>,
    ) -> Result<HashMap<i64, HashMap<JobStatus, i64>>> {
        let rows = self
            .0
            .connect_and_read(|connection| -> Result<Vec<(i64, JobStatus, i64)>> {
                let (predicate, value) = job_type.map_or(("!=", -1), |i| ("=", i.id()));
                query_with_params(
                    &format!(
                        r#"
SELECT
    job_type,
    status,
    COUNT(*) AS count
FROM jobby_jobs
WHERE job_type {predicate} ?
GROUP BY job_type, status
ORDER BY job_type, status
;"#
                    ),
                    connection,
                    &(value),
                )
                .map_err(Error::JobMetrics)
            })
            .await
            .map_err(Error::ConnectionFetching)??;
        Ok(rows
            .into_iter()
            .group_by(|row| row.0)
            .into_iter()
            .map(|(type_, group)| {
                let map: HashMap<JobStatus, i64> = group.map(|row| (row.1, row.2)).collect();
                (type_, map)
            })
            .collect())
    }

    async fn job_timing_metrics(
        &self,
        job_type: UnnamespacedJobType,
    ) -> Result<(i64, i64, i64, i64)> {
        Ok(self
            .0
            .connect_and_read(|connection| -> Result<(i64, i64, i64, i64)> {
                query_optional_with_params(
                    r"
SELECT
    job_type,
    CAST(COALESCE(AVG(ready_at - queue_time), 0) AS INTEGER),
    CAST(COALESCE(AVG(
        CASE 
            WHEN dequeue_time IS NULL THEN NULL
            ELSE dequeue_time - ready_at
        END
    ), 0) AS INTEGER),
    CAST(COALESCE(AVG(
        CASE 
            WHEN completion_time IS NULL THEN NULL
            ELSE completion_time - dequeue_time
        END
    ), 0) AS INTEGER)
FROM jobby_jobs
WHERE job_type = ?
GROUP BY job_type
;",
                    connection,
                    &(job_type),
                )
                .map_err(Error::JobMetrics)
                .map(std::option::Option::unwrap_or_default)
            })
            .await
            .map_err(Error::ConnectionFetching)??)
    }

    async fn job_sizing_metrics(
        &self,
        job_type: UnnamespacedJobType,
    ) -> Result<Vec<(BlobIsInputOrOutput, i64, i64)>> {
        Ok(self
            .0
            .connect_and_read(
                |connection| -> Result<Vec<(BlobIsInputOrOutput, i64, i64)>> {
                    query_with_params(
                        r"
SELECT
    is_input,
    CAST(COALESCE(AVG(length(contents)), 0) AS INTEGER),
    CAST(COALESCE(AVG(length(metadata)), 0) AS INTEGER)
FROM jobby_blob_store
WHERE job_type = ?
GROUP BY is_input
;",
                        connection,
                        &(job_type),
                    )
                    .map_err(Error::JobMetrics)
                },
            )
            .await
            .map_err(Error::ConnectionFetching)??)
    }

    async fn job_type_recent_jobs(
        &self,
        job_type: UnnamespacedJobType,
        limit: usize,
        status: JobStatus,
    ) -> Result<Vec<JobFullState>> {
        Ok(self
            .0
            .connect_and_read(|connection| -> Result<Vec<JobFullState>> {
                query_with_params(
                    r"
SELECT *
FROM jobby_jobs
WHERE job_type = ?
AND   status = ?
ORDER BY COALESCE(completion_time, queue_time) DESC
LIMIT ?
;",
                    connection,
                    &(job_type, status, limit),
                )
                .map_err(Error::JobMetrics)
            })
            .await
            .map_err(Error::ConnectionFetching)??)
    }

    async fn job_full_state(
        &self,
        job_type: UnnamespacedJobType,
        job_id: JobId,
    ) -> Result<JobFullStateWithData> {
        Ok(self
            .0
            .connect_and_read(|connection| -> Result<JobFullStateWithData> {
                query_optional_with_params(
                    r"
WITH input AS (
    SELECT job_type, id, metadata, contents, updated_time, expires_at
    FROM jobby_blob_store
    WHERE is_input
    AND job_type = ?
    AND id = ?
), 
output AS (
    SELECT job_type, id, metadata, contents, updated_time, expires_at
    FROM jobby_blob_store
    WHERE is_input = FALSE
    AND job_type = ?
    AND id = ?
)
SELECT 
    jobby_jobs.job_type AS job_type,
    jobby_jobs.id AS id,
    dependency,
    dependency_type,
    status,
    queue_time,
    ready_at,
    dequeue_time,
    completion_time,
    allowed_attempts,
    attempts,
    error_message,
    expiry,
    jobby_jobs.expires_at AS expires_at,
    input.metadata AS input_metadata,
    input.contents AS input_contents,
    input.updated_time AS input_updated_time,
    input.expires_at AS input_expires_at,
    output.metadata AS output_metadata,
    output.contents AS output_contents,
    output.updated_time AS output_updated_time,
    output.expires_at AS output_expires_at
FROM jobby_jobs
JOIN input ON jobby_jobs.job_type = input.job_type
LEFT JOIN output ON jobby_jobs.job_type = output.job_type
WHERE jobby_jobs.job_type = ? 
AND jobby_jobs.id = ?
;",
                    connection,
                    &(job_type, &job_id, job_type, &job_id, job_type, &job_id),
                )
                .map_err(Error::JobMetrics)?
                .ok_or_else(|| Error::JobNotFound(job_id, job_type.id()))
            })
            .await
            .map_err(Error::ConnectionFetching)??)
    }

    async fn mark_job_ready(&self, job_type: UnnamespacedJobType, job_id: JobId) -> Result<()> {
        Ok(self
            .0
            .connect_and_write(
                WriteAuthorization::IPromiseThisIsABackgroundJobNotTiedToARequest,
                |transaction| -> Result<()> {
                    execute_with_params(
                        r"
UPDATE jobby_jobs
SET ready_at = ?
WHERE job_type = ?
AND id = ?
AND status = ?
;",
                        &transaction,
                        &(Self::now(), job_type, job_id, JobStatus::Queued),
                    )
                    .map_err(Error::JobInsertion)?;
                    transaction.commit().map_err(Error::JobInsertion)?;
                    Ok(())
                },
            )
            .await
            .map_err(Error::ConnectionFetching)??)
    }

    async fn force_run_job_now(&self, job_type: UnnamespacedJobType, job_id: JobId) -> Result<()> {
        Ok(self
            .0
            .connect_and_write(
                WriteAuthorization::IPromiseThisIsABackgroundJobNotTiedToARequest,
                |transaction| -> Result<()> {
                    execute_with_params(
                        r"
UPDATE jobby_jobs
SET ready_at = ?,
    status = ?
WHERE job_type = ?
AND id = ?
;",
                        &transaction,
                        &(Self::now(), JobStatus::Queued, job_type, job_id),
                    )
                    .map_err(Error::JobInsertion)?;
                    transaction.commit().map_err(Error::JobInsertion)?;
                    Ok(())
                },
            )
            .await
            .map_err(Error::ConnectionFetching)??)
    }

    async fn search_jobs(
        &self,
        prefix: String,
        job_type: Option<UnnamespacedJobType>,
    ) -> Result<Vec<(i64, String, JobStatus)>> {
        Ok(self
            .0
            .connect_and_read(|connection| -> Result<Vec<(i64, String, JobStatus)>> {
                let (predicate, value) = job_type.map_or(("!=", -1), |i| ("=", i.id()));
                query_with_params(
                    &format!(
                        r#"
SELECT
    job_type,
    id,
    status
FROM jobby_jobs
WHERE job_type {predicate} ?
AND id LIKE ?
;"#
                    ),
                    connection,
                    &(value, prefix),
                )
                .map_err(Error::JobSearching)
            })
            .await
            .map_err(Error::ConnectionFetching)??)
    }

    async fn fetch_dag(
        &self,
        job_type: UnnamespacedJobType,
        job_id: JobId,
    ) -> Result<Vec<JobStateForDAG>> {
        self.0
            .connect_and_read(move |connection| -> Result<Vec<JobStateForDAG>> {
                Self::fetch_dag_inner(connection, job_type, job_id)
            })
            .await
            .map_err(Error::ConnectionFetching)?
    }
}
