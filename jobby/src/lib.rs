#![warn(clippy::all, clippy::pedantic, clippy::nursery)]
#![allow(
    clippy::missing_errors_doc,
    clippy::module_name_repetitions,
    clippy::must_use_candidate,
    // too many linter FPs: https://github.com/rust-lang/rust-clippy/issues/9271
    clippy::missing_const_for_fn,
    // Need a lot of u64 <-> i64 due to sqlite
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    clippy::struct_field_names
)]

mod action;
mod admin;
mod blob;
mod blob_encoding;
mod builder;
mod config;
mod db;
mod error;
mod executors;
mod jobby;
mod module;
mod rocket_stage;
mod sqlite_store;
mod states;
mod store;

const FUTURES_IN_PARALLEL: usize = 8;

pub use inventory;
pub use rocket;

pub use crate::jobby::{JobToBackfill, JobToSubmit, Jobby};
pub use action::Actions;
pub use admin::{JobDataFetchType, JobbyAdminHelper};
pub use blob::{IsAlreadyEncoded, JobbyBlob};
pub use blob_encoding::BlobCompressionAlgorithm;
pub use builder::{JobBuilder, JobRegistry, WorkerRegistry};
pub use config::{JobCreationMode, WorkerCreationMode};
pub use error::{BoxDynError, Error};
pub use executors::*;
pub use jobby_derive::JobType;
pub use module::{Client, ClientModule, JobType, JobTypeMetadata, Module};
pub use rocket_stage::stage;
pub use sqlite_store::SqliteJobbyStore;
pub use states::{BlobIsInputOrOutput, JobId, UnnamespacedJobType};
