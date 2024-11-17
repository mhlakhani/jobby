use crate::{config::WorkerCreationMode, Error, JobCreationMode, JobRegistry, WorkerRegistry};

use rocket::{Build, Rocket};
use serde::Serialize;

// A JobType suitable for declaring and using jobs.
// Do not implement this directly! Just declare a #[repr(u8)]
// enum, give values to all the discriminants, and use the derive macro
pub trait JobType: Into<u8> + Into<&'static str> {
    // ID for this client. Must be globally unique
    // Each client gets upto 256 job types
    // We can eventually add more in the future, this is just easier

    fn client_id() -> usize;

    // List out the metadatas for each job type.
    fn list_metadata() -> Vec<JobTypeMetadata>;
}

#[derive(Debug, Clone, Serialize)]
pub struct JobTypeMetadata {
    pub unnamespaced_id: i64,
    pub base_id: u8,
    pub client_id: usize,
    pub name: &'static str,
    pub client_name: &'static str,
    pub is_submittable: bool,
}

#[async_trait::async_trait]
pub trait Module {
    fn initialize(rocket: &Rocket<Build>) -> Result<Self, Error>
    where
        Self: Sized;

    // Register workers for this module, when running in the given mode
    async fn workers(
        &self,
        registry: WorkerRegistry,
        mode: WorkerCreationMode,
    ) -> Result<(), Error>;
    // Register jobs for this module, when running in the given mode
    async fn jobs(&self, registry: JobRegistry, mode: JobCreationMode) -> Result<(), Error>;
}

type ModuleInitializer = fn(&Rocket<Build>) -> Result<Box<dyn Module + Send + Sync>, Error>;
type JobMetadataLister = fn() -> Vec<JobTypeMetadata>;

pub struct ClientModule {
    pub(crate) id: usize,
    pub(crate) name: &'static str,
    pub(crate) module_initializer: ModuleInitializer,
    pub(crate) job_metadata_lister: JobMetadataLister,
}

impl ClientModule {
    pub const fn new(
        id: usize,
        name: &'static str,
        module_initializer: ModuleInitializer,
        job_metadata_lister: JobMetadataLister,
    ) -> Self {
        Self {
            id,
            name,
            module_initializer,
            job_metadata_lister,
        }
    }
}

inventory::collect!(ClientModule);

#[derive(Debug, Clone, Serialize)]
pub struct Client {
    pub id: usize,
    pub name: &'static str,
}

impl From<&ClientModule> for Client {
    fn from(module: &ClientModule) -> Self {
        Self {
            id: module.id,
            name: module.name,
        }
    }
}
