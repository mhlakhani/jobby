use crate::{
    action::Actions,
    blob_encoding::BlobMetadata,
    states::{
        BlobIsInputOrOutput, JobFullState, JobFullStateWithData, JobStateAndWorkerId,
        JobStateForDAG, JobStatus, UnnamespacedJobType, UnnamespacedWorkerSpec, WorkerId,
    },
    JobId,
};

use std::{
    collections::HashMap,
    time::{Duration, SystemTime},
};

type Result<T, E = crate::Error> = anyhow::Result<T, E>;

// For submissions
#[derive(Debug)]
pub struct SubmittableJob {
    pub(crate) job_type: UnnamespacedJobType,
    pub(crate) id: JobId,
    pub(crate) data: (BlobMetadata, Vec<u8>),
    pub(crate) allowed_attempts: i64,
    pub(crate) dependency: Option<JobId>,
    pub(crate) dependency_type: Option<UnnamespacedJobType>,
    pub(crate) expiry: Option<Duration>,
}

// For backfills
#[derive(Debug)]
pub struct BackfillableJob {
    pub(crate) job_type: UnnamespacedJobType,
    pub(crate) id: JobId,
    pub(crate) data: Option<(BlobMetadata, Vec<u8>)>,
    pub(crate) ready_at: SystemTime,
}

// On job success
#[derive(Debug)]
pub enum HandleJobCompletionStatus {
    Success(Actions),
    Failure(String),
}

/// JobStore. Povides fetching and other information
#[async_trait::async_trait]
pub trait JobbyStore {
    async fn handle_job_completion(
        &self,
        job_type: UnnamespacedJobType,
        job_id: JobId,
        worker_id: WorkerId,
        status: HandleJobCompletionStatus,
    ) -> Result<()>;

    async fn submit_jobs(&self, jobs: Vec<SubmittableJob>) -> Result<()>;

    async fn fetch_and_claim_jobs(&self) -> Result<Vec<JobStateAndWorkerId>>;

    async fn fetch_job_data(
        &self,
        job_type: UnnamespacedJobType,
        id: JobId,
        is_input: BlobIsInputOrOutput,
    ) -> Result<Option<(BlobMetadata, Vec<u8>, i64)>>;

    async fn startup(&self) -> Result<()>;

    async fn register_workers(&self, workers: Vec<UnnamespacedWorkerSpec>) -> Result<()>;

    async fn backfill_jobs(&self, jobs: Vec<BackfillableJob>) -> Result<()>;

    async fn expire_jobs(&self) -> Result<()>;

    async fn expire_job(
        &self,
        job_type: UnnamespacedJobType,
        job_id: JobId,
        duration: Duration,
    ) -> Result<()>;
    async fn unexpire_job(&self, job_type: UnnamespacedJobType, job_id: JobId) -> Result<()>;

    // if ID is set, only return metrics for that ID
    async fn job_status_metrics(
        &self,
        job_type: Option<UnnamespacedJobType>,
    ) -> Result<HashMap<i64, HashMap<JobStatus, i64>>>;

    // Returns job_type, average ready at time, average queue time, average run time
    async fn job_timing_metrics(
        &self,
        job_type: UnnamespacedJobType,
    ) -> Result<(i64, i64, i64, i64)>;

    async fn job_sizing_metrics(
        &self,
        job_type: UnnamespacedJobType,
    ) -> Result<Vec<(BlobIsInputOrOutput, i64, i64)>>;

    async fn job_type_recent_jobs(
        &self,
        job_type: UnnamespacedJobType,
        limit: usize,
        status: JobStatus,
    ) -> Result<Vec<JobFullState>>;

    async fn job_full_state(
        &self,
        job_type: UnnamespacedJobType,
        job_id: JobId,
    ) -> Result<JobFullStateWithData>;

    // Marks a job as ready to run iff it's queued
    async fn mark_job_ready(&self, job_type: UnnamespacedJobType, job_id: JobId) -> Result<()>;

    // Force set a job to be ready to run now. Only for debugging
    async fn force_run_job_now(&self, job_type: UnnamespacedJobType, job_id: JobId) -> Result<()>;

    // Search jobs with the given type (if provided) and the ID prefix
    async fn search_jobs(
        &self,
        prefix: String,
        job_type: Option<UnnamespacedJobType>,
    ) -> Result<Vec<(i64, String, JobStatus)>>;

    // Load the dag that this node is a part of
    async fn fetch_dag(
        &self,
        job_type: UnnamespacedJobType,
        job_id: JobId,
    ) -> Result<Vec<JobStateForDAG>>;
}
