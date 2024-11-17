use crate::{
    builder::JobBuilder, jobby::JobTypeResolver, states::UnnamespacedJobType, JobId, JobType,
    JobbyBlob,
};

use std::time::{Duration, SystemTime};

type Result<T, E = crate::Error> = anyhow::Result<T, E>;

// Actions that can be taken as a result of completing this job
#[derive(Debug)]
pub struct Actions {
    pub(crate) job_type: UnnamespacedJobType,
    pub(crate) job_id: JobId,
    pub(crate) expiry: Option<Duration>,
    pub(crate) writes: Vec<JobbyBlob>,
    pub(crate) jobs: Vec<JobBuilder>,
    pub(crate) resolver: JobTypeResolver,
}

impl Actions {
    pub(crate) const fn new(
        resolver: JobTypeResolver,
        job_type: UnnamespacedJobType,
        job_id: JobId,
        expiry: Option<Duration>,
    ) -> Self {
        Self {
            job_type,
            job_id,
            expiry,
            writes: vec![],
            jobs: vec![],
            resolver,
        }
    }

    // TODO: Add helper that takes the inputs
    pub fn write_data(&mut self, blob: JobbyBlob) {
        self.writes.push(blob);
    }

    // Submit a job that does not depend on this one, inheriting the expiry by default
    pub fn submit_job<T: JobType>(&self, job_type: T, id: JobId) -> JobBuilder {
        JobBuilder::new(job_type, id).expiry(self.expiry).submit()
    }

    // Submit a job that depends on the current one
    pub fn submit_dependent_job<T: JobType>(&self, job_type: T, id: JobId) -> JobBuilder {
        JobBuilder::new(job_type, id)
            .unnamespaced_dependency(self.job_type, self.job_id.clone())
            .expiry(self.expiry)
            .submit()
    }

    // Submit a job with the type resolved at runtime. Can fail.
    // Default semantics are the same as submit_job, with an additional constraint
    // that the job type must be submittable
    pub fn submit_job_dynamic(
        &self,
        client_name: &str,
        job_type_name: &str,
        id: JobId,
    ) -> Result<JobBuilder> {
        let job_type = self
            .resolver
            .resolve_for_submission(client_name, job_type_name)?;
        Ok(JobBuilder::new_unnamespaced(job_type, id)
            .expiry(self.expiry)
            .submit())
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
