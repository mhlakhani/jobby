// States and status enums
use crate::{JobType, JobTypeMetadata};

use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use strum_macros::{FromRepr, IntoStaticStr};

/// States a worker can be in
#[derive(
    Debug, Clone, Copy, Ord, PartialEq, PartialOrd, Eq, FromRepr, Serialize_repr, Deserialize_repr,
)]
#[repr(i64)]
pub enum WorkerStatus {
    WaitingForJob,
    RunningJob,
}

/// States a job can be in
#[derive(
    Debug,
    Clone,
    Copy,
    Ord,
    PartialEq,
    PartialOrd,
    Eq,
    FromRepr,
    Serialize_repr,
    Deserialize_repr,
    Hash,
    IntoStaticStr,
)]
#[repr(i64)]
pub enum JobStatus {
    Queued,
    WaitingForDependency,
    Dequeued,
    Succeeded,
    Failed,
}

// Can only be created from a namespaced one to avoid confusion
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct UnnamespacedJobType(i64);

impl<T> From<T> for UnnamespacedJobType
where
    T: JobType,
{
    fn from(type_: T) -> Self {
        #[allow(clippy::cast_possible_wrap)]
        let offset = (T::client_id() as i64) * 1000;
        let id_u8: u8 = type_.into();
        let id: i64 = offset + i64::from(id_u8);
        Self(id)
    }
}

impl From<&JobTypeMetadata> for UnnamespacedJobType {
    fn from(metadata: &JobTypeMetadata) -> Self {
        Self(metadata.unnamespaced_id)
    }
}

impl UnnamespacedJobType {
    pub const fn id(&self) -> i64 {
        self.0
    }
}

/// A `JobId` represents a unique identifier (within a type) for a job
/// Use fixed IDs if you want to be able to rerun jobs again after,
/// or random IDs for things you don't care about
pub type JobId = String;

// Same but for workers
pub type WorkerId = String;

/// State for a job used for scheduling.
#[derive(Debug, Deserialize)]
pub struct JobState {
    pub(crate) job_type: UnnamespacedJobType,
    pub(crate) id: JobId,
    pub(crate) dependency: Option<JobId>,
    pub(crate) dependency_type: Option<UnnamespacedJobType>,
    // In ms
    pub(crate) expiry: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct JobAssignment {
    pub(crate) job_id: JobId,
    pub(crate) worker_id: WorkerId,
    pub(crate) job_type: UnnamespacedJobType,
}

#[derive(Debug)]
pub struct JobStateAndWorkerId {
    pub job: JobState,
    pub worker_id: WorkerId,
}

#[derive(Debug, Deserialize)]
pub struct JobTypeAndWorkerId {
    pub(crate) job_type: UnnamespacedJobType,
    pub(crate) worker_id: WorkerId,
}

#[derive(Debug)]
pub struct UnnamespacedWorkerSpec {
    pub id: JobId,
    pub job_type: UnnamespacedJobType,
    pub fetch_interval: Duration,
    // TODO: Validate this must be nonzero
    pub fetch_jitter: Duration,
    pub last_fetch_time: Option<SystemTime>,
}

#[derive(Debug, Clone, Copy, Hash, Serialize_repr, Deserialize_repr, PartialEq, Eq)]
#[repr(i64)]
pub enum BlobIsInputOrOutput {
    Output = 0,
    Input = 1,
}

// Full job state for analytics
#[derive(Debug, Serialize, Deserialize)]
pub struct JobFullState {
    pub(crate) job_type: i64,
    pub(crate) id: JobId,
    pub(crate) dependency: Option<JobId>,
    pub(crate) dependency_type: Option<i64>,
    pub(crate) status: JobStatus,
    pub(crate) queue_time: i64,
    pub(crate) ready_at: i64,
    pub(crate) dequeue_time: Option<i64>,
    pub(crate) completion_time: Option<i64>,
    pub(crate) allowed_attempts: i64,
    pub(crate) attempts: i64,
    pub(crate) error_message: Option<String>,
    pub(crate) expiry: Option<i64>,
    pub(crate) expires_at: Option<i64>,
}

// Full job state with data, for admin panel
#[derive(Debug, Deserialize)]
pub struct JobFullStateWithData {
    pub(crate) job_type: i64,
    pub(crate) id: JobId,
    pub(crate) dependency: Option<JobId>,
    pub(crate) dependency_type: Option<i64>,
    pub(crate) status: JobStatus,
    pub(crate) queue_time: i64,
    pub(crate) ready_at: i64,
    pub(crate) dequeue_time: Option<i64>,
    pub(crate) completion_time: Option<i64>,
    pub(crate) allowed_attempts: i64,
    pub(crate) attempts: i64,
    pub(crate) error_message: Option<String>,
    pub(crate) expiry: Option<i64>,
    pub(crate) expires_at: Option<i64>,
    pub(crate) input_metadata: String,
    pub(crate) input_contents: Vec<u8>,
    pub(crate) input_updated_time: i64,
    pub(crate) input_expires_at: Option<i64>,
    pub(crate) output_metadata: Option<String>,
    pub(crate) output_contents: Option<Vec<u8>>,
    pub(crate) output_updated_time: Option<i64>,
    pub(crate) output_expires_at: Option<i64>,
}

// Simplified job state for dependency graphs
#[derive(Debug, Deserialize)]
pub struct JobStateForDAG {
    pub job_type: i64,
    pub id: JobId,
    pub dependency: Option<JobId>,
    pub dependency_type: Option<i64>,
    pub status: JobStatus,
}
