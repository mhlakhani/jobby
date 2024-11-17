use crate::{
    states::{UnnamespacedJobType, UnnamespacedWorkerSpec, WorkerId},
    store::{BackfillableJob, SubmittableJob},
    Actions, Error, JobId, JobType, JobbyBlob, Worker,
};

use std::{
    collections::HashMap,
    ops::Add,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use futures::StreamExt;

type Result<T, E = crate::Error> = anyhow::Result<T, E>;

// TODO: log if dropped w/o building?
#[derive(Debug)]
pub struct JobBuilder {
    // always needed
    job_type: UnnamespacedJobType,
    id: JobId,
    // used for submits and backfills - can be none on a backfill
    data: Option<JobbyBlob>,
    // needed for backfills - if this is set, we're backfilling.
    ready_at: Option<SystemTime>,
    // needed for submits
    is_submit: bool,
    // defaults to 1
    allowed_attempts: i64,
    dependency: Option<JobId>,
    dependency_type: Option<UnnamespacedJobType>,
    expiry: Option<Duration>,
}

impl JobBuilder {
    #[must_use]
    pub fn new<T: JobType>(job_type: T, id: JobId) -> Self {
        Self::new_unnamespaced(UnnamespacedJobType::from(job_type), id)
    }

    #[must_use]
    pub(crate) fn new_unnamespaced(job_type: UnnamespacedJobType, id: JobId) -> Self {
        Self {
            job_type,
            id,
            data: None,
            ready_at: None,
            is_submit: false,
            allowed_attempts: 1,
            dependency: None,
            dependency_type: None,
            expiry: None,
        }
    }

    #[must_use]
    pub fn data(self, data: JobbyBlob) -> Self {
        Self {
            data: Some(data),
            ..self
        }
    }

    #[must_use]
    pub fn backfill_at(self, ready_at: SystemTime) -> Self {
        Self {
            ready_at: Some(ready_at),
            ..self
        }
    }

    #[must_use]
    pub fn backfill_in(self, ready_in: Duration) -> Self {
        Self {
            ready_at: Some(SystemTime::now().add(ready_in)),
            ..self
        }
    }

    #[must_use]
    pub fn submit(self) -> Self {
        Self {
            is_submit: true,
            ..self
        }
    }

    #[must_use]
    pub fn allowed_attempts(self, allowed_attempts: i64) -> Self {
        Self {
            allowed_attempts,
            ..self
        }
    }

    #[must_use]
    pub fn dependency<T: JobType>(self, dependency_type: T, dependency: JobId) -> Self {
        Self {
            dependency: Some(dependency),
            dependency_type: Some(UnnamespacedJobType::from(dependency_type)),
            ..self
        }
    }

    #[must_use]
    pub(crate) fn unnamespaced_dependency(
        self,
        dependency_type: UnnamespacedJobType,
        dependency: JobId,
    ) -> Self {
        Self {
            dependency: Some(dependency),
            dependency_type: Some(dependency_type),
            ..self
        }
    }

    #[must_use]
    pub fn expiry(self, expiry: Option<Duration>) -> Self {
        Self { expiry, ..self }
    }

    #[must_use]
    pub fn without_expiry(self) -> Self {
        Self {
            expiry: None,
            ..self
        }
    }

    #[must_use]
    pub fn expires_in(self, expires_in: Duration) -> Self {
        Self {
            expiry: Some(expires_in),
            ..self
        }
    }

    // TODO: See if we should run the validations upfront. Probably should
    pub fn build(self, actions: &mut Actions) {
        actions.jobs.push(self);
    }

    // TODO: See if we should run the validations upfront. Probably should
    // TODO: See if we can name this better
    pub fn register(self, registry: &JobRegistry) {
        registry.register(self);
    }

    pub(crate) async fn into_jobs(
        self,
    ) -> Result<(Option<SubmittableJob>, Option<BackfillableJob>)> {
        let data = if let Some(blob) = self.data {
            Some(blob.encode().await?.into_parts())
        } else {
            None
        };
        let backfill = if let Some(ready_at) = self.ready_at {
            Some(BackfillableJob {
                job_type: self.job_type,
                id: self.id.clone(),
                data: data.clone(),
                ready_at,
            })
        } else {
            None
        };
        let submit = self.is_submit.then(|| {
            let data = data.unwrap_or_else(JobbyBlob::empty_parts);
            SubmittableJob {
                job_type: self.job_type,
                id: self.id.clone(),
                data,
                allowed_attempts: self.allowed_attempts,
                dependency: self.dependency,
                dependency_type: self.dependency_type,
                expiry: self.expiry,
            }
        });
        Ok((submit, backfill))
    }

    pub(crate) async fn bulk_into_jobs(
        builders: Vec<Self>,
    ) -> Result<(Vec<BackfillableJob>, Vec<SubmittableJob>)> {
        if builders.is_empty() {
            return Ok((vec![], vec![]));
        }
        let mut backfills: Vec<BackfillableJob> = vec![];
        let mut submits: Vec<SubmittableJob> = vec![];
        let futures = builders.into_iter().map(Self::into_jobs);
        let stream = futures::stream::iter(futures).buffered(crate::FUTURES_IN_PARALLEL);
        let maybe_jobses = stream.collect::<Vec<_>>().await;
        let jobses = maybe_jobses.into_iter().collect::<Result<Vec<_>>>()?;
        for (maybe_submit, maybe_backfill) in jobses {
            if let Some(submit) = maybe_submit {
                submits.push(submit);
            }
            if let Some(backfill) = maybe_backfill {
                backfills.push(backfill);
            }
        }
        Ok((backfills, submits))
    }
}

pub struct WorkerRegistryStorage {
    pub(crate) workers: HashMap<WorkerId, Worker>,
    pub(crate) specs: Vec<UnnamespacedWorkerSpec>,
}

impl WorkerRegistryStorage {
    pub(crate) fn new() -> Self {
        Self {
            workers: HashMap::new(),
            specs: vec![],
        }
    }

    fn register<T: JobType>(
        &mut self,
        worker: Worker,
        id: WorkerId,
        job_type: T,
        fetch_interval: Duration,
        fetch_jitter: Duration,
        last_fetch_time: Option<SystemTime>,
    ) -> Result<()> {
        if self.workers.contains_key(&id) {
            return Err(Error::WorkerAlreadyRegistered(id));
        }
        self.workers.insert(id.clone(), worker);
        self.specs.push(UnnamespacedWorkerSpec {
            id,
            job_type: UnnamespacedJobType::from(job_type),
            fetch_interval,
            fetch_jitter,
            last_fetch_time,
        });
        Ok(())
    }
}

pub struct WorkerRegistry {
    client_name: &'static str,
    storage: Arc<Mutex<WorkerRegistryStorage>>,
}

impl WorkerRegistry {
    pub(crate) fn new(
        client_name: &'static str,
        storage: Arc<Mutex<WorkerRegistryStorage>>,
    ) -> Self {
        Self {
            client_name,
            storage,
        }
    }

    /// # Panics
    ///
    /// Will panic if mutex is poisoned
    #[allow(clippy::unwrap_in_result)]
    pub fn register<T: JobType>(
        &self,
        worker: Worker,
        id: &str,
        job_type: T,
        fetch_interval: Duration,
        fetch_jitter: Duration,
        // You mostly want to set this to `None` which will set it to now + fetch_interval
        // to avoid running a ton of jobs immediately on startup
        last_fetch_time: Option<SystemTime>,
    ) -> Result<()> {
        let id = format!("{}_{id}", self.client_name);
        self.storage.lock().expect("mutex not poisoned").register(
            worker,
            id,
            job_type,
            fetch_interval,
            fetch_jitter,
            last_fetch_time,
        )
    }
}

pub struct JobRegistry {
    storage: Arc<Mutex<Vec<JobBuilder>>>,
}

impl JobRegistry {
    pub(crate) fn new(storage: Arc<Mutex<Vec<JobBuilder>>>) -> Self {
        Self { storage }
    }

    pub(crate) fn register(&self, builder: JobBuilder) {
        self.storage
            .lock()
            .expect("mutex not poisoned")
            .push(builder);
    }

    pub fn submit_job<T: JobType>(&self, job_type: T, id: JobId) -> JobBuilder {
        JobBuilder::new(job_type, id).submit()
    }

    pub fn backfill_job_at<T: JobType>(
        &self,
        job_type: T,
        id: JobId,
        ready_at: SystemTime,
    ) -> JobBuilder {
        JobBuilder::new(job_type, id).backfill_at(ready_at)
    }

    pub fn backfill_job_in<T: JobType>(
        &self,
        job_type: T,
        id: JobId,
        ready_in: Duration,
    ) -> JobBuilder {
        JobBuilder::new(job_type, id).backfill_in(ready_in)
    }
}
