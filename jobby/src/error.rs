use crate::{states::WorkerId, JobId};

pub type BoxDynError = Box<dyn std::error::Error + Send + Sync>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Inserting Blob: {0:?}")]
    BlobInsertion(rusqlite::Error),
    #[error("Inserting Blob for id {0:?}: {1:?}")]
    BlobFetching(JobId, rusqlite::Error),
    #[error("Blob not found for id {0:?} and type {1:?}")]
    BlobNotFound(JobId, i64),
    #[error("Inserting Job: {0:?}")]
    JobInsertion(rusqlite::Error),
    #[error("JobFetchingAndClaiming: {0:?}")]
    JobFetchingAndClaiming(rusqlite::Error),
    #[error("JobFetchingAndClaimingMismatchedLengths: {0:?} workers - {1:?} states")]
    JobFetchingAndClaimingMismatchedLengths(usize, usize),
    #[error("JobFetchingAndClaimingMismatchedJobType: no worker found for type {0:?}")]
    JobFetchingAndClaimingMismatchedJobType(i64),
    #[error("Marking success of job with id: {0:?} - {1:?}")]
    JobSuccessMarking(JobId, rusqlite::Error),
    #[error("Marking failure of job with id: {0:?} - {1:?}")]
    JobFailureMarking(JobId, rusqlite::Error),
    #[error("Resolving Dependencies of job with id: {0:?} - {1:?}")]
    JobDependencyResolving(JobId, rusqlite::Error),
    #[error("Backfilling jobs - {0:?}")]
    JobBackfilling(rusqlite::Error),
    #[error("Startup: {0:?}")]
    Startup(rusqlite::Error),
    #[error("Expiry: {0:?}")]
    Expiry(rusqlite::Error),
    #[error("Registering worker with id: {0:?}")]
    WorkerRegistration(rusqlite::Error),
    #[error("Freeing worker with id: {0:?} - {1:?}")]
    WorkerFreeing(WorkerId, rusqlite::Error),
    #[error("serde: {0:?}")]
    Serde(#[from] serde_json::Error),
    #[error("Already have worker with id: {0:?}")]
    WorkerAlreadyRegistered(WorkerId),
    #[error("Error executing job: {0:?}")]
    JobExecution(BoxDynError),
    #[error("Error initializing module: {0:?}")]
    ModuleInitialization(BoxDynError),
    #[error("SystemTime: {0:?}")]
    SystemTime(#[from] std::time::SystemTimeError),
    #[error("JobbyBlob cannot be encoded without compression")]
    JobbyBlobUnexpectedEncodedData,
    #[error("async join: {0:?}")]
    AsyncJoin(#[from] tokio::task::JoinError),
    #[error("Data en/decoding: {0:?}")]
    Decoding(#[from] std::io::Error),
    #[error("Creating jobby without workers!")]
    CreatingJobbyWithoutWorkers,
    #[error("Error fetching data of type {0:?} for id {1:?}: {2:?}")]
    URLFetchAndStoreJobError(String, JobId, String),
    #[error("No dependency data found for job of type {0:?}")]
    NoDependencyDataFoundForJob(JobId),
    #[error("db pool: {0:?}")]
    ConnectionFetching(rocket_sqlite_rw_pool::Error),
    #[error("unknown: {0:?}")]
    Unknown(BoxDynError),
    #[error("Duplicate namespaces with the same client ID {0}: {1} and {2}!")]
    DuplicateClientId(usize, &'static str, &'static str),
    #[error("Duplicate namespaces with the same client name {0}: {1} and {2}!")]
    DuplicateClientName(&'static str, usize, usize),
    #[error("JobMetrics: {0:?}")]
    JobMetrics(rusqlite::Error),
    #[error("JobMetricsComputationReturnedNoRow for: {0:?}")]
    JobMetricsComputationReturnedNoRow(i64),
    #[error("Job not found for id {0:?} and type {1:?}")]
    JobNotFound(JobId, i64),
    #[error("Invalid job type: {0:?}")]
    InvalidJobType(i64),
    #[error("Searching jobs: {0:?}")]
    JobSearching(rusqlite::Error),
    #[error("Fetching data for job: {0:?}")]
    JobDataFetching(BoxDynError),
    #[error("Loading job DAG: {0:?}")]
    JobDagFetching(rusqlite::Error),
    #[error("Unknown client: {0}")]
    UnknownClient(String),
    #[error("Unknown job type {0} for client {1}")]
    UnknownJobTypeForClient(String, String),
    #[error("Job type {0} for client {1} is not submittable")]
    JobTypeIsNotSubmittable(String, String),
    #[error("Configuration: {0}")]
    Configuration(#[from] rocket::figment::Error),
}