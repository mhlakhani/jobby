use crate::{
    builder::WorkerRegistryStorage,
    config::Config,
    module::{Client, ClientModule},
    states::{BlobIsInputOrOutput, JobStateAndWorkerId, UnnamespacedJobType, WorkerId},
    store::{BackfillableJob, HandleJobCompletionStatus, JobbyStore, SubmittableJob},
    Actions, Error, IsAlreadyEncoded, JobBuilder, JobId, JobRegistry, JobType, JobTypeMetadata,
    JobbyAdminHelper, JobbyBlob, Worker, WorkerRegistry,
};

use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
    sync::{atomic::AtomicBool, Arc, Mutex},
    time::{Duration, Instant, SystemTime},
};

use futures::StreamExt;
use itertools::Itertools;
use rocket::{Build, Rocket};

type Result<T, E = crate::Error> = anyhow::Result<T, E>;

// For submissions
#[derive(Debug)]
pub struct JobToSubmit<T: JobType> {
    pub job_type: T,
    pub id: JobId,
    pub allowed_attempts: i64,
    pub data: JobbyBlob,
    pub dependency: Option<JobId>,
    pub dependency_type: Option<T>,
    // How long after success should the job be deleted?
    // Note that failed jobs always stick around for debugging.
    // Note that it's possible a job can succeed but have an expired parent
    // so it can't be rerun. We intentionally do not prevent this.
    pub expiry: Option<Duration>,
}

// For backfills
#[derive(Debug)]
pub struct JobToBackfill<T: JobType> {
    pub job_type: T,
    pub id: JobId,
    pub data: Option<JobbyBlob>,
    pub ready_at: SystemTime,
}

/// Metadata about the jobby setup. Useful for admin
pub struct JobbyMetadata {
    pub(crate) clients: Vec<Client>,
    pub(crate) job_type_id_to_metadata: HashMap<i64, JobTypeMetadata>,
    pub(crate) client_id_to_job_types: HashMap<usize, Vec<JobTypeMetadata>>,
}

impl JobbyMetadata {
    fn new() -> Self {
        Self {
            clients: vec![],
            job_type_id_to_metadata: HashMap::new(),
            client_id_to_job_types: HashMap::new(),
        }
    }

    fn register(&mut self, module: &ClientModule) {
        let jobs = (module.job_metadata_lister)();
        let client: Client = module.into();
        for job in &jobs {
            self.job_type_id_to_metadata
                .insert(job.unnamespaced_id, job.clone());
        }
        self.client_id_to_job_types.insert(client.id, jobs);
        self.clients.push(client);
    }
}

/// Resolver for creating dynamic jobs
#[derive(Clone, Debug)]
pub struct JobTypeResolver {
    mapping: Arc<HashMap<String, HashMap<String, JobTypeMetadata>>>,
}

impl JobTypeResolver {
    fn new(metadata: &JobbyMetadata) -> Self {
        let mapping: HashMap<String, HashMap<String, JobTypeMetadata>> = metadata
            .clients
            .iter()
            .map(|client| {
                let job_name_to_metadata = metadata
                    .client_id_to_job_types
                    .get(&client.id)
                    .map(|entries| {
                        entries
                            .iter()
                            .map(|job_metadata| {
                                (job_metadata.name.to_owned(), job_metadata.clone())
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                (client.name.to_owned(), job_name_to_metadata)
            })
            .collect();
        let mapping = Arc::new(mapping);
        Self { mapping }
    }

    fn resolve(&self, client_name: &str, job_type_name: &str) -> Result<&JobTypeMetadata> {
        self.mapping
            .get(client_name)
            .map(|for_client| for_client.get(job_type_name))
            .ok_or_else(|| Error::UnknownClient(client_name.to_string()))?
            .ok_or_else(|| {
                Error::UnknownJobTypeForClient(job_type_name.to_string(), client_name.to_string())
            })
    }

    pub(crate) fn resolve_for_submission(
        &self,
        client_name: &str,
        job_type_name: &str,
    ) -> Result<UnnamespacedJobType> {
        let metadata = self.resolve(client_name, job_type_name)?;
        if metadata.is_submittable {
            Ok(UnnamespacedJobType::from(metadata))
        } else {
            Err(Error::JobTypeIsNotSubmittable(
                job_type_name.to_string(),
                client_name.to_string(),
            ))
        }
    }
}

/// Jobby. The job manager
pub struct Jobby {
    store: Arc<dyn JobbyStore + Send + Sync>,
    shutdown: Arc<AtomicBool>,
    metadata: JobbyMetadata,
}

impl Jobby {
    pub(crate) async fn new(
        rocket: &Rocket<Build>,
        store: Arc<dyn JobbyStore + Send + Sync>,
    ) -> Result<Self> {
        let config = Config::from(rocket)?;
        store.startup().await?;
        let mut client_map = HashMap::new();
        let mut client_names = HashMap::new();
        let mut metadata = JobbyMetadata::new();
        let workers = Arc::new(Mutex::new(WorkerRegistryStorage::new()));
        let job_builders = Arc::new(Mutex::new(vec![]));
        for client in inventory::iter::<ClientModule> {
            println!(
                "jobby: registering client {} with id {}",
                client.name, client.id
            );
            if let Some(existing) = client_map.insert(client.id, client.name) {
                return Err(Error::DuplicateClientId(client.id, existing, client.name));
            }
            if let Some(existing) = client_names.insert(client.name, client.id) {
                return Err(Error::DuplicateClientName(client.name, client.id, existing));
            }
            metadata.register(client);
            let module = (client.module_initializer)(rocket)?;
            let registry = WorkerRegistry::new(client.name, Arc::clone(&workers));
            module
                .workers(registry, config.worker_creation_mode)
                .await?;
            let registry = JobRegistry::new(Arc::clone(&job_builders));
            module.jobs(registry, config.job_creation_mode).await?;
        }
        let specs = std::mem::take(
            &mut workers
                .lock()
                .expect("mutex not poisoned")
                .deref_mut()
                .specs,
        );
        store.register_workers(specs).await?;
        let workers_map = std::mem::take(
            &mut workers
                .lock()
                .expect("mutex not poisoned")
                .deref_mut()
                .workers,
        );
        let shutdown = Arc::new(AtomicBool::new(false));
        let resolver = JobTypeResolver::new(&metadata);
        Self::spawn_work_in_background(
            &config,
            workers_map,
            Arc::clone(&store),
            resolver,
            Arc::clone(&shutdown),
        );
        let builders: Vec<JobBuilder> =
            std::mem::take(&mut *job_builders.lock().expect("mutex not poisoned"));
        let (backfillable, submittable) = JobBuilder::bulk_into_jobs(builders).await?;
        let me = Self {
            store,
            shutdown,
            metadata,
        };
        me.backfill_jobs_inner(backfillable).await?;
        me.submit_jobs_inner(submittable).await?;
        Ok(me)
    }

    pub fn admin(&self) -> JobbyAdminHelper<'_> {
        JobbyAdminHelper::new(self, Arc::clone(&self.store), &self.metadata)
    }

    fn spawn_work_in_background(
        config: &Config,
        workers: HashMap<WorkerId, Worker>,
        store: Arc<dyn JobbyStore + Send + Sync>,
        resolver: JobTypeResolver,
        shutdown: Arc<AtomicBool>,
    ) {
        {
            let wait_time = Duration::from_millis(config.job_run_interval_ms);
            let shutdown = Arc::clone(&shutdown);
            let store = Arc::clone(&store);
            tokio::spawn(async move {
                while !shutdown.load(std::sync::atomic::Ordering::SeqCst) {
                    tokio::time::sleep(wait_time).await;
                    if let Err(e) =
                        Self::run_single_cycle(&workers, Arc::clone(&store), resolver.clone()).await
                    {
                        // TODO: trace?
                        println!("Error running cycle: {e:?}");
                    }
                }
            });
        }
        let wait_time = Duration::from_millis(config.job_expiry_interval_ms);
        tokio::spawn(async move {
            while !shutdown.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::time::sleep(wait_time).await;
                if let Err(e) = store.expire_jobs().await {
                    // TODO: trace?
                    println!("Error expiring jobs: {e:?}");
                }
            }
        });
    }

    async fn fetch_and_decode(
        store: &(dyn JobbyStore + Send + Sync),
        job_type: UnnamespacedJobType,
        job_id: JobId,
        is_input: BlobIsInputOrOutput,
    ) -> Result<Option<Vec<u8>>> {
        let result = store
            .fetch_job_data(job_type, job_id.clone(), is_input)
            .await?;
        if let Some((metadata, data, _)) = result {
            // Values in DB must have been encoded
            let blob: JobbyBlob = (metadata, data, IsAlreadyEncoded::Yes).into();
            Ok(Some(blob.decode().await?.take()?))
        } else {
            Ok(None)
        }
    }

    async fn run_single_job_fallible(
        executor: Worker,
        store: Arc<dyn JobbyStore + Send + Sync>,
        resolver: JobTypeResolver,
        assignment: JobStateAndWorkerId,
    ) -> Result<()> {
        let dependency_data = if let Some(dependency) = assignment.job.dependency {
            if let Some(dependency_type) = assignment.job.dependency_type {
                Self::fetch_and_decode(
                    &*store,
                    dependency_type,
                    dependency,
                    BlobIsInputOrOutput::Output,
                )
                .await?
            } else {
                None
            }
        } else {
            None
        };
        let Some(data) = Self::fetch_and_decode(
            &*store,
            assignment.job.job_type,
            assignment.job.id.clone(),
            BlobIsInputOrOutput::Input,
        )
        .await?
        else {
            return Err(Error::BlobNotFound(
                assignment.job.id.clone(),
                assignment.job.job_type.id(),
            ));
        };
        let mut actions = Actions::new(
            resolver,
            assignment.job.job_type,
            assignment.job.id.clone(),
            assignment
                .job
                .expiry
                .map(|expiry| Duration::from_millis(expiry as u64)),
        );
        let result = executor
            .execute(
                assignment.job.id.clone(),
                data,
                dependency_data,
                &mut actions,
            )
            .await;
        // TODO: Make sure we free the worker even if there are errors here
        match result {
            Ok(()) => {
                store
                    .handle_job_completion(
                        assignment.job.job_type,
                        assignment.job.id.clone(),
                        assignment.worker_id.clone(),
                        HandleJobCompletionStatus::Success(actions),
                    )
                    .await
            }
            Err(e) => {
                store
                    .handle_job_completion(
                        assignment.job.job_type,
                        assignment.job.id.clone(),
                        assignment.worker_id.clone(),
                        HandleJobCompletionStatus::Failure(e.to_string()),
                    )
                    .await?;
                Err(Error::JobExecution(e))
            }
        }
    }

    async fn run_single_job_infallible(
        executor: Worker,
        store: Arc<dyn JobbyStore + Send + Sync>,
        resolver: JobTypeResolver,
        assignment: JobStateAndWorkerId,
    ) {
        // TODO: Trace?
        let id = assignment.job.id.clone();
        let job_type = assignment.job.job_type;
        let start = Instant::now();
        println!(
            "Running job {:?} of type {} on worker {}",
            id,
            job_type.id(),
            assignment.worker_id,
        );
        if let Err(e) = Self::run_single_job_fallible(executor, store, resolver, assignment).await {
            // TODO: trace?
            println!("Error running job {id:?} (type {job_type:?}): {e:?}");
        } else {
            let duration = start.elapsed().as_millis();
            println!(
                "Successfully ran job {:?} of type {} in {} ms (e2e)",
                id,
                job_type.id(),
                duration
            );
        }
    }

    async fn run_single_cycle(
        workers: &HashMap<WorkerId, Worker>,
        store: Arc<dyn JobbyStore + Send + Sync>,
        resolver: JobTypeResolver,
    ) -> Result<()> {
        let assignments = store.fetch_and_claim_jobs().await?;
        if assignments.is_empty() {
            return Ok(());
        }
        // TODO: Trace?
        println!("Executing {} assignments", assignments.len());
        // always spawn, worker will stay busy this way and won't block others
        for assignment in assignments {
            if let Some(executor) = workers.get(&assignment.worker_id) {
                let executor = Arc::clone(executor);
                let store = Arc::clone(&store);
                let resolver = resolver.clone();
                tokio::spawn(async move {
                    Self::run_single_job_infallible(executor, store, resolver, assignment).await;
                });
            } else {
                // TODO: Trace?
                println!("No worker found for id {}", assignment.worker_id);
            }
        }
        Ok(())
    }

    pub fn shutdown(&self) {
        self.shutdown
            .deref()
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    async fn make_job_submittable<T: JobType + Send>(
        job: JobToSubmit<T>,
    ) -> Result<SubmittableJob> {
        let data = job.data.encode().await?.into_parts();
        Ok(SubmittableJob {
            job_type: UnnamespacedJobType::from(job.job_type),
            id: job.id,
            data,
            allowed_attempts: job.allowed_attempts,
            dependency: job.dependency,
            dependency_type: job.dependency_type.map(UnnamespacedJobType::from),
            expiry: job.expiry,
        })
    }

    pub(crate) async fn make_jobs_submittable<T: JobType + Send>(
        jobs: Vec<JobToSubmit<T>>,
    ) -> Result<Vec<SubmittableJob>> {
        if jobs.is_empty() {
            return Ok(vec![]);
        }
        let futures = jobs.into_iter().map(Self::make_job_submittable);
        let stream = futures::stream::iter(futures).buffered(crate::FUTURES_IN_PARALLEL);
        let maybe_submittable = stream.collect::<Vec<_>>().await;
        let submittable = maybe_submittable
            .into_iter()
            .collect::<Result<Vec<SubmittableJob>>>()?;
        Ok(submittable)
    }

    pub(crate) async fn submit_jobs_inner(&self, submittable: Vec<SubmittableJob>) -> Result<()> {
        if submittable.is_empty() {
            return Ok(());
        }
        // TODO: Trace?
        println!(
            "Submitting jobs with ids {:?}",
            submittable
                .iter()
                .map(|s| format!("({}, {:?})", s.id.as_str(), s.job_type))
                .join(",")
        );
        self.store.submit_jobs(submittable).await?;
        Ok(())
    }

    // Submit a set of jobs with associated data. No-op for any existing jobs
    pub async fn submit_jobs<T: JobType + Send>(&self, jobs: Vec<JobToSubmit<T>>) -> Result<()> {
        if jobs.is_empty() {
            return Ok(());
        }
        let submittable = Self::make_jobs_submittable(jobs).await?;
        self.submit_jobs_inner(submittable).await
    }

    async fn make_job_backfillable<T: JobType + Send>(
        job: JobToBackfill<T>,
    ) -> Result<BackfillableJob> {
        let data = if let Some(data) = job.data {
            Some(data.encode().await?.into_parts())
        } else {
            None
        };
        Ok(BackfillableJob {
            job_type: UnnamespacedJobType::from(job.job_type),
            id: job.id,
            data,
            ready_at: job.ready_at,
        })
    }

    pub(crate) async fn make_jobs_backfillable<T: JobType + Send>(
        backfills: Vec<JobToBackfill<T>>,
    ) -> Result<Vec<BackfillableJob>> {
        if backfills.is_empty() {
            return Ok(vec![]);
        }
        let futures = backfills.into_iter().map(Self::make_job_backfillable);
        let stream = futures::stream::iter(futures).buffered(crate::FUTURES_IN_PARALLEL);
        let maybe_backfillable = stream.collect::<Vec<_>>().await;
        let backfillable = maybe_backfillable
            .into_iter()
            .collect::<Result<Vec<BackfillableJob>>>()?;
        Ok(backfillable)
    }

    pub(crate) async fn backfill_jobs_inner(
        &self,
        backfillable: Vec<BackfillableJob>,
    ) -> Result<()> {
        if backfillable.is_empty() {
            return Ok(());
        }
        // TODO: Trace?
        println!(
            "Backfilling jobs with ids {:?}",
            backfillable
                .iter()
                .map(|s| format!("({}, {:?})", s.id.as_str(), s.job_type))
                .join(",")
        );
        self.store.backfill_jobs(backfillable).await?;
        Ok(())
    }

    pub async fn backfill_jobs<T: JobType + Send>(
        &self,
        backfills: Vec<JobToBackfill<T>>,
    ) -> Result<()> {
        if backfills.is_empty() {
            return Ok(());
        }
        let backfillable = Self::make_jobs_backfillable(backfills).await?;
        self.backfill_jobs_inner(backfillable).await
    }
}
