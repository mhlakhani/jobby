use rocket::{
    figment::{providers::Serialized, Error, Figment},
    Build, Rocket,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub enum JobCreationMode {
    // Run no jobs (default)
    None,
    // Jobs for local testing
    Local,
    // Jobs for production
    Production,
}

impl Default for JobCreationMode {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub enum WorkerCreationMode {
    // Run no workers (default)
    None,
    // Workers for local testing
    Local,
    // Workers for production
    Production,
}

impl Default for WorkerCreationMode {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub(crate) job_creation_mode: JobCreationMode,
    pub(crate) worker_creation_mode: WorkerCreationMode,
    pub(crate) job_run_interval_ms: u64,
    pub(crate) job_expiry_interval_ms: u64,
}

impl Config {
    pub(crate) fn from(rocket: &Rocket<Build>) -> Result<Self, Error> {
        Self::figment(rocket).extract::<Self>()
    }

    fn figment(rocket: &Rocket<Build>) -> Figment {
        let figment = Figment::from(rocket.figment())
            .focus("jobby")
            .join(Serialized::default(
                "job_creation_mode",
                JobCreationMode::default(),
            ))
            .join(Serialized::default(
                "worker_creation_mode",
                WorkerCreationMode::default(),
            ))
            .join(Serialized::default("job_run_interval_ms", 100_u64))
            .join(Serialized::default("job_expiry_interval_ms", 10000_u64));

        figment
    }
}
