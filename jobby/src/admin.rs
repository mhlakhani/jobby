use crate::{
    blob_encoding::BlobMetadata,
    jobby::JobbyMetadata,
    states::{JobFullState, JobFullStateWithData, JobStatus},
    store::{BackfillableJob, JobbyStore, SubmittableJob},
    BlobIsInputOrOutput, Client, Error, IsAlreadyEncoded, JobId, JobTypeMetadata, Jobby, JobbyBlob,
    UnnamespacedJobType,
};

use std::{
    collections::{HashMap, HashSet},
    fmt::Write,
    sync::Arc,
    time::{Duration, SystemTime},
};

use anyhow::anyhow;
use humantime::format_duration;
use itertools::Itertools;
use rocket::{
    request::{FromRequest, Outcome, Request},
    State,
};
use serde::Serialize;

type Result<T, E = crate::Error> = anyhow::Result<T, E>;

#[derive(Debug, Serialize)]
pub struct JobStatusMetricsRow {
    succeeded: i64,
    failed: i64,
    queued: i64,
    dequeued: i64,
    waiting_for_dependency: i64,
}

impl JobStatusMetricsRow {
    fn from(map: impl Iterator<Item = (JobStatus, i64)>) -> Self {
        let mut succeeded = 0;
        let mut failed = 0;
        let mut queued = 0;
        let mut dequeued = 0;
        let mut waiting_for_dependency = 0;

        for (status, v) in map {
            match status {
                JobStatus::Dequeued => {
                    dequeued = v;
                }
                JobStatus::Queued => {
                    queued = v;
                }
                JobStatus::WaitingForDependency => {
                    waiting_for_dependency = v;
                }
                JobStatus::Succeeded => {
                    succeeded = v;
                }
                JobStatus::Failed => {
                    failed = v;
                }
            }
        }

        Self {
            succeeded,
            failed,
            queued,
            dequeued,
            waiting_for_dependency,
        }
    }
}

// TODO: Think about whether we need a metric for
// average attempts to success

#[derive(Debug, Serialize)]
pub struct JobTimingMetricsRow {
    // Time between queue_time and ready_at
    average_ready_at_delay: String,
    // Time between ready_at and dequeue
    average_time_to_dequeue: String,
    // Time between dequeue and completion
    average_time_to_complete: String,
}

impl JobTimingMetricsRow {
    fn from(values: (i64, i64, i64, i64)) -> Self {
        Self {
            average_ready_at_delay: format_duration(Duration::from_millis(values.1 as u64))
                .to_string(),
            average_time_to_dequeue: format_duration(Duration::from_millis(values.2 as u64))
                .to_string(),
            average_time_to_complete: format_duration(Duration::from_millis(values.3 as u64))
                .to_string(),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct JobSizingMetricsRow {
    input_contents: i64,
    input_metadata: i64,
    output_contents: i64,
    output_metadata: i64,
}

impl JobSizingMetricsRow {
    fn from(rows: Vec<(BlobIsInputOrOutput, i64, i64)>) -> Self {
        let mut input_contents = 0;
        let mut input_metadata = 0;
        let mut output_contents = 0;
        let mut output_metadata = 0;

        for (is_input, contents, metadata) in rows {
            match is_input {
                BlobIsInputOrOutput::Input => {
                    input_contents = contents;
                    input_metadata = metadata;
                }
                BlobIsInputOrOutput::Output => {
                    output_contents = contents;
                    output_metadata = metadata;
                }
            }
        }

        Self {
            input_contents,
            input_metadata,
            output_contents,
            output_metadata,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct JobCompletionInfoRow {
    id: JobId,
    completion_time: String,
    run_time: String,
    attempts: i64,
}

impl JobCompletionInfoRow {
    fn from(state: JobFullState) -> Self {
        let completion_time =
            chrono::NaiveDateTime::from_timestamp_opt(state.completion_time.unwrap_or(0) / 1000, 0)
                .map_or_else(
                    || "Unknown".to_string(),
                    |naive_datetime| format!("{}", naive_datetime.format("%Y-%m-%d %H:%M:%S")),
                );
        Self {
            id: state.id,
            attempts: state.attempts,
            completion_time,
            run_time: format_duration(Duration::from_millis(
                (state.completion_time.unwrap_or(0) as u64)
                    - (state.dequeue_time.unwrap_or(0) as u64),
            ))
            .to_string(),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct JobRunningInfoRow {
    id: JobId,
    dequeue_time: String,
    run_time: String,
    attempts: i64,
    allowed_attempts: i64,
}

impl JobRunningInfoRow {
    fn from(state: JobFullState) -> Self {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0))
            .as_millis() as u64;
        Self {
            id: state.id,
            attempts: state.attempts,
            allowed_attempts: state.allowed_attempts,
            dequeue_time: format_duration(Duration::from_millis(
                state.dequeue_time.unwrap_or(0) as u64
            ))
            .to_string(),
            run_time: format_duration(Duration::from_millis(
                now - (state.dequeue_time.unwrap_or(0) as u64),
            ))
            .to_string(),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct JobWaitingInfoRow {
    id: JobId,
    queue_time: String,
    ready_in: String,
}

impl JobWaitingInfoRow {
    fn from(state: JobFullState) -> Self {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0))
            .as_millis() as u64;
        let ready_at = state.ready_at as u64;
        let ready_in = if ready_at > now {
            format_duration(Duration::from_millis(ready_at - now)).to_string()
        } else {
            format_duration(Duration::from_millis(now - ready_at)).to_string() + " ago"
        };
        Self {
            id: state.id,
            queue_time: format_duration(Duration::from_millis(now - (state.queue_time as u64)))
                .to_string(),
            ready_in,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct JobData {
    pub is_input: String,
    pub compressed_size: usize,
    pub decompressed_size: usize,
    // Contents in printable form, trimmed to 2KB
    pub contents: String,
    pub updated_at: String,
    pub expires_at: String,
}

impl JobData {
    // TODO: At some future point, push this down to the DB so we don't
    // read large blobs to throw most of the data away
    fn content_string(data: &[u8]) -> String {
        // We convert then trim, to avoid issues encoding due to our trimming
        // Yes this is slow but hey this UI isn't called often, we can optimize later
        std::str::from_utf8(data)
            .map(|s| s[0..s.len().min(2048)].to_owned())
            .ok()
            .unwrap_or_else(|| "( binary data )".to_owned())
    }

    fn expires_at_string(now: u64, time: i64) -> String {
        format_duration(Duration::from_millis((time as u64) - now)).to_string()
    }

    fn updated_at_string(now: u64, time: i64) -> String {
        format_duration(Duration::from_millis(now - (time as u64))).to_string() + " ago"
    }

    async fn from(is_input: BlobIsInputOrOutput, state: &mut JobFullStateWithData) -> Option<Self> {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0))
            .as_millis() as u64;
        let (contents, updated_at, expires_at, compressed_size, decompressed_size) = match is_input
        {
            BlobIsInputOrOutput::Input => {
                let input = std::mem::take(&mut state.input_contents);
                let compressed_size = input.len();
                let metadata: BlobMetadata = serde_json::from_str(&state.input_metadata).ok()?;
                // Values in DB must have been encoded
                let blob: JobbyBlob = (metadata, input, IsAlreadyEncoded::Yes).into();
                let input = blob.decode().await.ok()?.take().ok()?;
                let decompressed_size = input.len();
                (
                    Some(Self::content_string(&input)),
                    Some(Self::updated_at_string(now, state.input_updated_time)),
                    state
                        .input_expires_at
                        .map(|time| Self::expires_at_string(now, time)),
                    compressed_size,
                    decompressed_size,
                )
            }
            BlobIsInputOrOutput::Output => {
                let compressed_size = state
                    .output_contents
                    .as_ref()
                    .map(std::vec::Vec::len)
                    .unwrap_or_default();
                let output = match (state.output_contents.take(), state.output_metadata.take()) {
                    (Some(output), Some(metadata)) => {
                        let metadata: BlobMetadata = serde_json::from_str(&metadata).ok()?;
                        // Values in DB must have been encoded
                        let blob: JobbyBlob = (metadata, output, IsAlreadyEncoded::Yes).into();

                        blob.decode().await.ok()?.take().ok()?
                    }
                    _ => {
                        return None;
                    }
                };
                let decompressed_size = output.len();
                (
                    Some(Self::content_string(&output)),
                    state
                        .output_updated_time
                        .map(|time| Self::updated_at_string(now, time)),
                    state
                        .output_expires_at
                        .map(|time| Self::expires_at_string(now, time)),
                    compressed_size,
                    decompressed_size,
                )
            }
        };
        let is_input = match is_input {
            BlobIsInputOrOutput::Input => "Input".to_owned(),
            BlobIsInputOrOutput::Output => "Output".to_owned(),
        };
        Some(Self {
            is_input,
            compressed_size,
            decompressed_size,
            contents: contents.unwrap_or_else(|| "( null )".to_owned()),
            updated_at: updated_at.unwrap_or_else(|| "N/A".to_owned()),
            expires_at: expires_at.unwrap_or_else(|| "N/A".to_owned()),
        })
    }
}

#[derive(Debug, Serialize)]
pub struct AnnotatedJobState {
    pub job_type: i64,
    pub job_type_name: String,
    pub job_type_client_name: String,
    pub id: JobId,
    pub dependency: Option<JobId>,
    // name of the type
    pub dependency_type: Option<i64>,
    pub dependency_type_name: Option<String>,
    pub dependency_type_client_name: Option<String>,
    pub status: &'static str,
    pub queue_time: String,
    pub ready_in: String,
    pub ready_at_in_past: bool,
    pub dequeue_time: Option<String>,
    pub completion_time: Option<String>,
    pub allowed_attempts: i64,
    pub attempts: i64,
    pub error_message: Option<String>,
    pub expiry: Option<String>,
    pub expires_at: Option<String>,
    pub input_data: JobData,
    pub output_data: Option<JobData>,
}

impl AnnotatedJobState {
    fn format_time(now: i64, then: i64) -> String {
        let now = now as u64;
        let then = then as u64;
        if now < then {
            format_duration(Duration::from_millis(then - now)).to_string()
        } else {
            format_duration(Duration::from_millis(now - then)).to_string() + " ago"
        }
    }

    async fn from(metadata: &JobbyMetadata, mut state: JobFullStateWithData) -> Result<Self> {
        let (job_type_name, job_type_client_name) = metadata
            .job_type_id_to_metadata
            .get(&state.job_type)
            .map(|metadata| (metadata.name.to_owned(), metadata.client_name.to_owned()))
            .ok_or(Error::InvalidJobType(state.job_type))?;
        let (dependency_type_name, dependency_type_client_name) = match state
            .dependency_type
            .map(|type_id| {
                metadata
                    .job_type_id_to_metadata
                    .get(&type_id)
                    .map(|metadata| (metadata.name.to_owned(), metadata.client_name.to_owned()))
                    .ok_or(Error::InvalidJobType(state.job_type))
            })
            .transpose()?
        {
            Some((name, client_name)) => (Some(name), Some(client_name)),
            None => (None, None),
        };
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0))
            .as_millis() as i64;
        // TODO: In theory we should do these in parallel, but eh...
        let input_data = JobData::from(BlobIsInputOrOutput::Input, &mut state).await;
        let output_data = JobData::from(BlobIsInputOrOutput::Output, &mut state).await;
        let expiry = state
            .expiry
            .map(|expiry| format_duration(Duration::from_millis(expiry as u64)).to_string());
        let input_data =
            input_data.ok_or_else(|| Error::BlobNotFound(state.id.clone(), state.job_type))?;
        let ready_at_in_past = state.ready_at < now;
        Ok(Self {
            job_type: state.job_type,
            job_type_name,
            job_type_client_name,
            id: state.id,
            dependency: state.dependency,
            dependency_type: state.dependency_type,
            dependency_type_name,
            dependency_type_client_name,
            status: state.status.into(),
            queue_time: Self::format_time(now, state.queue_time),
            ready_in: Self::format_time(now, state.ready_at),
            ready_at_in_past,
            dequeue_time: state.dequeue_time.map(|time| Self::format_time(now, time)),
            completion_time: state
                .completion_time
                .map(|time| Self::format_time(now, time)),
            allowed_attempts: state.allowed_attempts,
            attempts: state.attempts,
            error_message: state.error_message,
            expiry,
            expires_at: state.expires_at.map(|time| Self::format_time(now, time)),
            input_data,
            output_data,
        })
    }
}

#[derive(Debug, Serialize)]
pub struct JobSearchRow {
    job_type: JobTypeMetadata,
    id: JobId,
    status: &'static str,
}

impl JobSearchRow {
    fn from(metadata: &JobbyMetadata, row: (i64, String, JobStatus)) -> Result<Self> {
        let job_type = metadata
            .job_type_id_to_metadata
            .get(&row.0)
            .cloned()
            .ok_or(Error::InvalidJobType(row.0))?;
        Ok(Self {
            job_type,
            id: row.1,
            status: row.2.into(),
        })
    }

    pub fn id(&self) -> JobId {
        self.id.clone()
    }

    pub fn job_type_id(&self) -> i64 {
        self.job_type.unnamespaced_id
    }
}

#[derive(Debug)]
pub enum JobDataFetchType {
    Input,
    Output,
    Error,
}

impl<'r> rocket::request::FromParam<'r> for JobDataFetchType {
    type Error = &'r str;

    fn from_param(param: &'r str) -> Result<Self, &'r str> {
        match param {
            "input" => Ok(Self::Input),
            "output" => Ok(Self::Output),
            "error" => Ok(Self::Error),
            _ => Err("Unexpected data fetch type"),
        }
    }
}

// Admin UI for jobby
pub struct JobbyAdminHelper<'a> {
    _jobby: &'a Jobby,
    store: Arc<dyn JobbyStore + Send + Sync>,
    metadata: &'a JobbyMetadata,
}

impl<'a> JobbyAdminHelper<'a> {
    pub(crate) fn new(
        jobby: &'a Jobby,
        store: Arc<dyn JobbyStore + Send + Sync>,
        metadata: &'a JobbyMetadata,
    ) -> Self {
        Self {
            _jobby: jobby,
            store,
            metadata,
        }
    }

    pub fn client_names(&self) -> Vec<String> {
        self.metadata
            .clients
            .iter()
            .map(|c| c.name.to_owned())
            .collect()
    }

    pub fn clients(&self) -> &[Client] {
        &self.metadata.clients
    }

    pub fn job_types(&self) -> Vec<JobTypeMetadata> {
        let mut types: Vec<_> = self
            .metadata
            .job_type_id_to_metadata
            .values()
            .cloned()
            .collect();
        types.sort_by_key(|type_| type_.unnamespaced_id);
        types
    }

    pub async fn total_job_status_metrics(
        &self,
    ) -> Result<(
        i64,
        JobStatusMetricsRow,
        Vec<(i64, &'static str, JobStatusMetricsRow)>,
    )> {
        let mut total: i64 = 0;
        let mut per_status = HashMap::new();
        let mut per_job = self.store.job_status_metrics(None).await?;
        let per_job =
            self.metadata
                .job_type_id_to_metadata
                .iter()
                .map(|(job_type_id, metadata)| {
                    let job_type_name = metadata.name;
                    let by_status = JobStatusMetricsRow::from(
                        per_job.entry(*job_type_id).or_default().iter_mut().map(
                            |(status, count)| {
                                total += *count;
                                *per_status.entry(*status).or_default() += *count;
                                (*status, *count)
                            },
                        ),
                    );
                    (*job_type_id, job_type_name, by_status)
                })
                .collect();
        let per_status = JobStatusMetricsRow::from(per_status.into_iter());
        Ok((total, per_status, per_job))
    }

    pub async fn job_status_metrics(
        &self,
        job_type: &JobTypeMetadata,
    ) -> Result<(i64, JobStatusMetricsRow)> {
        let job_type = UnnamespacedJobType::from(job_type);
        let mut per_job = self.store.job_status_metrics(Some(job_type)).await?;
        let this_job = per_job.entry(job_type.id()).or_default();
        let total = this_job.values().sum();
        let row = JobStatusMetricsRow::from(this_job.iter_mut().map(|(k, v)| (*k, *v)));
        Ok((total, row))
    }

    pub async fn job_timing_metrics(
        &self,
        job_type: &JobTypeMetadata,
    ) -> Result<JobTimingMetricsRow> {
        self.store
            .job_timing_metrics(job_type.into())
            .await
            .map(JobTimingMetricsRow::from)
    }

    pub async fn job_sizing_metrics(
        &self,
        job_type: &JobTypeMetadata,
    ) -> Result<JobSizingMetricsRow> {
        self.store
            .job_sizing_metrics(job_type.into())
            .await
            .map(JobSizingMetricsRow::from)
    }

    pub async fn recent_running(
        &self,
        job_type: &JobTypeMetadata,
        limit: usize,
    ) -> Result<Vec<JobRunningInfoRow>> {
        Ok(self
            .store
            .job_type_recent_jobs(job_type.into(), limit, JobStatus::Dequeued)
            .await?
            .into_iter()
            .map(JobRunningInfoRow::from)
            .collect())
    }

    pub async fn recent_successes(
        &self,
        job_type: &JobTypeMetadata,
        limit: usize,
    ) -> Result<Vec<JobCompletionInfoRow>> {
        Ok(self
            .store
            .job_type_recent_jobs(job_type.into(), limit, JobStatus::Succeeded)
            .await?
            .into_iter()
            .map(JobCompletionInfoRow::from)
            .collect())
    }

    pub async fn recent_failures(
        &self,
        job_type: &JobTypeMetadata,
        limit: usize,
    ) -> Result<Vec<JobCompletionInfoRow>> {
        Ok(self
            .store
            .job_type_recent_jobs(job_type.into(), limit, JobStatus::Failed)
            .await?
            .into_iter()
            .map(JobCompletionInfoRow::from)
            .collect())
    }

    pub async fn recent_waiting(
        &self,
        job_type: &JobTypeMetadata,
        limit: usize,
    ) -> Result<Vec<JobWaitingInfoRow>> {
        let (queued, waiting) = futures::try_join!(
            self.store
                .job_type_recent_jobs(job_type.into(), limit, JobStatus::Queued),
            self.store.job_type_recent_jobs(
                job_type.into(),
                limit,
                JobStatus::WaitingForDependency
            )
        )?;
        Ok(queued
            .into_iter()
            .chain(waiting.into_iter())
            .sorted_by_key(|r| r.completion_time)
            .take(limit)
            .map(JobWaitingInfoRow::from)
            .collect())
    }

    pub fn job_type(&self, id: i64) -> Option<&JobTypeMetadata> {
        self.metadata.job_type_id_to_metadata.get(&id)
    }

    // Pulls out data to show a summary.
    // Does not contain the full data (intentionally) to avoid expensive page renders
    pub async fn fetch_job(
        &self,
        job_type: &JobTypeMetadata,
        job_id: JobId,
    ) -> Result<AnnotatedJobState> {
        let state = self.store.job_full_state(job_type.into(), job_id).await?;
        AnnotatedJobState::from(self.metadata, state).await
    }

    // Pulls out the job data and error messages
    pub async fn fetch_job_data(
        &self,
        job_type: &JobTypeMetadata,
        job_id: JobId,
        data_type: JobDataFetchType,
    ) -> Result<Vec<u8>> {
        let state = self.store.job_full_state(job_type.into(), job_id).await?;
        let (metadata, data) = match data_type {
            JobDataFetchType::Input => (state.input_metadata, state.input_contents),
            JobDataFetchType::Output => (
                state.output_metadata.ok_or_else(|| {
                    Error::JobDataFetching(anyhow!("No output metadata for this job!").into())
                })?,
                state.output_contents.ok_or_else(|| {
                    Error::JobDataFetching(anyhow!("No output contents for this job!").into())
                })?,
            ),
            JobDataFetchType::Error => {
                return state
                    .error_message
                    .map(|e| e.as_bytes().to_vec())
                    .ok_or_else(|| {
                        Error::JobDataFetching(anyhow!("No error for this job!").into())
                    });
            }
        };
        let metadata: BlobMetadata = serde_json::from_str(&metadata)?;
        // Values in DB must have been encoded
        let blob: JobbyBlob = (metadata, data, IsAlreadyEncoded::Yes).into();
        let data = blob.decode().await?.take()?;
        Ok(data)
    }

    pub async fn run_job_now(&self, job_type: &JobTypeMetadata, job_id: JobId) -> Result<()> {
        self.store.mark_job_ready(job_type.into(), job_id).await
    }

    pub async fn force_run_job_now(&self, job_type: &JobTypeMetadata, job_id: JobId) -> Result<()> {
        self.store.force_run_job_now(job_type.into(), job_id).await
    }

    pub async fn backfill_job_now(&self, job_type: &JobTypeMetadata, job_id: JobId) -> Result<()> {
        self.store
            .backfill_jobs(vec![BackfillableJob {
                job_type: job_type.into(),
                id: job_id,
                data: None,
                ready_at: SystemTime::now(),
            }])
            .await
    }

    pub async fn submit_job(
        &self,
        job_type: &JobTypeMetadata,
        job_id: JobId,
        expiry: Option<Duration>,
        backfill: bool,
    ) -> Result<()> {
        let data: JobbyBlob = ().into();
        let data = data.encode().await?.into_parts();
        self.store
            .submit_jobs(vec![SubmittableJob {
                job_type: job_type.into(),
                id: job_id.clone(),
                data,
                allowed_attempts: 1,
                dependency: None,
                dependency_type: None,
                expiry,
            }])
            .await?;
        if backfill {
            self.store
                .backfill_jobs(vec![BackfillableJob {
                    job_type: job_type.into(),
                    id: job_id,
                    data: None,
                    ready_at: SystemTime::now(),
                }])
                .await
        } else {
            Ok(())
        }
    }

    pub async fn expire_job(
        &self,
        job_type: &JobTypeMetadata,
        job_id: JobId,
        expiry: Duration,
    ) -> Result<()> {
        self.store.expire_job(job_type.into(), job_id, expiry).await
    }

    pub async fn unexpire_job(&self, job_type: &JobTypeMetadata, job_id: JobId) -> Result<()> {
        self.store.unexpire_job(job_type.into(), job_id).await
    }

    pub async fn search_jobs(
        &self,
        prefix: String,
        job_type: Option<&JobTypeMetadata>,
    ) -> Result<Vec<JobSearchRow>> {
        Ok(self
            .store
            .search_jobs(prefix, job_type.map(std::convert::Into::into))
            .await?
            .into_iter()
            .filter_map(|row| JobSearchRow::from(self.metadata, row).ok())
            .collect())
    }

    pub async fn visualize_dag<F>(
        &self,
        job_type: &JobTypeMetadata,
        job_id: JobId,
        router: F,
    ) -> Result<String>
    where
        F: Fn(i64, String) -> String + Send,
    {
        let unnamespaced: UnnamespacedJobType = job_type.into();
        let type_id = unnamespaced.id();
        let rows = self.store.fetch_dag(unnamespaced, job_id.clone()).await?;
        let mut graph = "digraph G {\n bgcolor = \"transparent\"\n".to_owned();
        let mut seen: HashSet<String> = HashSet::with_capacity(rows.len() * 2);
        // add the nodes one by one
        for row in &rows {
            let fillcolor = match row.status {
                JobStatus::Queued | JobStatus::WaitingForDependency => "lightslategray",
                JobStatus::Dequeued => "dodgerblue",
                JobStatus::Succeeded => "mediumseagreen",
                JobStatus::Failed => "orangered",
            };
            let fontname = if row.job_type == type_id && row.id == job_id {
                "Times-Bold"
            } else {
                "Times"
            };
            let key = format!("{}_{}", row.job_type, row.id).replace('"', "%22");
            seen.insert(key.clone());
            let metadata = self
                .job_type(row.job_type)
                .ok_or(Error::InvalidJobType(row.job_type))?;
            // Do we want typeid too?
            let _ = write!(
                graph,
                r#"
"{}" [shape=rectangle, style=filled, fillcolor={}, label="client: {}\ntype: {}\nid: {}\n",href="{}",fontname="{}",margin="0.22,0.1"];
"#,
                key,
                fillcolor,
                metadata.client_name,
                metadata.name,
                row.id.replace('"', "%22"),
                router(row.job_type, row.id.clone()).replace('"', "%22"),
                fontname
            );
        }
        // Add the edges one by one
        for row in rows {
            if let Some(dependency_id) = row.dependency {
                if let Some(dependency_type) = row.dependency_type {
                    let dependency_id = dependency_id.replace('"', "%22");
                    let dependency_key = format!("{dependency_type}_{dependency_id}");
                    // Dependency has been purged, so we need to add the node or it formats weirdly
                    if !seen.contains(&dependency_key) {
                        let _ = write!(
                            graph,
                            r#"
                            "{dependency_key}" [shape=rectangle, style=filled, fillcolor="lightslategray", label="type: {dependency_type}\nid: {dependency_id}\n",fontname="Times",margin="0.22,0.1"];
                            "#,
                        );
                    }
                    let _ = write!(
                        graph,
                        r#"
"{}" -> "{}_{}" [color="blue"];
"#,
                        dependency_key,
                        row.job_type,
                        row.id.replace('"', "%22")
                    );
                }
            }
        }
        graph.push_str("\n}");
        Ok(graph)
    }
}

#[async_trait::async_trait]
impl<'r> FromRequest<'r> for JobbyAdminHelper<'r> {
    type Error = ();

    async fn from_request(request: &'r Request<'_>) -> Outcome<Self, Self::Error> {
        match request.guard::<&State<Jobby>>().await {
            Outcome::Success(jobby) => Outcome::Success(jobby.admin()),
            Outcome::Error((status, ())) => Outcome::Error((status, ())),
            Outcome::Forward(f) => Outcome::Forward(f),
        }
    }
}
