use crate::{Actions, BlobCompressionAlgorithm, Error, IsAlreadyEncoded, JobId};

use serde::de::DeserializeOwned;

type Result<T, E = crate::error::BoxDynError> = anyhow::Result<T, E>;

// Executors and helpers to make them more easily
pub type Worker = std::sync::Arc<dyn JobExecutor + Send + Sync>;

#[async_trait::async_trait]
pub trait JobExecutor {
    // TODO: More trait wrappers for input/output
    async fn execute(
        &self,
        id: JobId,
        data: Vec<u8>,
        dependency_data: Option<Vec<u8>>,
        actions: &mut Actions,
    ) -> Result<()>;
}

#[async_trait::async_trait]
pub trait JobWithNoData {
    async fn execute(&self, id: JobId, actions: &mut Actions) -> Result<()>;
}
pub struct ExecutorForJobWithNoData<T>(pub T);

#[async_trait::async_trait]
pub trait JobWithNoDependencyData {
    async fn execute(&self, id: JobId, data: Vec<u8>, actions: &mut Actions) -> Result<()>;
}
pub struct ExecutorForJobWithNoDependencyData<T>(pub T);

// TODO: Take &self?
#[async_trait::async_trait]
pub trait URLFetchAndStoreJob {
    // TODO: API to take a full request if needed
    fn get_url(id: &str) -> Result<reqwest::Url>;
    // Add headers, build the request, etc
    fn build(id: &str, builder: reqwest::RequestBuilder) -> Result<reqwest::RequestBuilder>;
}

#[async_trait::async_trait]
pub trait URLFetchAndStoreJobWithData {
    // TODO: API to take a full request if needed
    fn get_url(id: &str, data: Vec<u8>) -> Result<reqwest::Url>;
    // Add headers, build the request, etc
    fn build(id: &str, builder: reqwest::RequestBuilder) -> Result<reqwest::RequestBuilder>;
}
pub struct ExecutorForURLFetchAndStoreJob<T>(pub T);

#[async_trait::async_trait]
pub trait URLPostAndStoreJob {
    // TODO: API to take a full request if needed
    fn get_url(&self, id: &str, data: &[u8]) -> Result<reqwest::Url>;
    // Add headers, build the request, etc
    fn build(
        &self,
        id: &str,
        data: &[u8],
        builder: reqwest::RequestBuilder,
    ) -> Result<reqwest::RequestBuilder>;
}
pub struct ExecutorForURLPostAndStoreJob<T>(pub T);

#[async_trait::async_trait]
pub trait JobWithDependencyData {
    async fn execute(
        &self,
        id: JobId,
        dependency_data: Vec<u8>,
        actions: &mut Actions,
    ) -> Result<()>;
}
pub struct ExecutorForJobWithDependencyData<T>(pub T);

// This requires DeserializeOwned, for a zero-copy version you'll have to
// implement JobWithDependencyData and deserialize it yourself right now
// as GATs aren't stable
#[async_trait::async_trait]
pub trait JobWithSerializedDependencyData {
    type Data;

    async fn execute(
        &self,
        id: JobId,
        dependency_data: Self::Data,
        actions: &mut Actions,
    ) -> Result<()>;
}
pub struct ExecutorForJobWithSerializedDependencyData<T>(pub T);

// Implementations

#[async_trait::async_trait]
impl<T> JobExecutor for ExecutorForJobWithNoData<T>
where
    T: JobWithNoData + Send + Sync,
{
    #[inline]
    async fn execute(
        &self,
        id: JobId,
        _data: Vec<u8>,
        _dependency_data: Option<Vec<u8>>,
        actions: &mut Actions,
    ) -> Result<()> {
        self.0.execute(id, actions).await
    }
}

#[async_trait::async_trait]
impl<T> JobExecutor for ExecutorForJobWithNoDependencyData<T>
where
    T: JobWithNoDependencyData + Send + Sync,
{
    #[inline]
    async fn execute(
        &self,
        id: JobId,
        data: Vec<u8>,
        _dependency_data: Option<Vec<u8>>,
        actions: &mut Actions,
    ) -> Result<()> {
        self.0.execute(id, data, actions).await
    }
}

impl<T> URLFetchAndStoreJobWithData for T
where
    T: URLFetchAndStoreJob + Send + Sync,
{
    fn get_url(id: &str, _data: Vec<u8>) -> Result<reqwest::Url> {
        <T as URLFetchAndStoreJob>::get_url(id)
    }

    fn build(id: &str, builder: reqwest::RequestBuilder) -> Result<reqwest::RequestBuilder> {
        <T as URLFetchAndStoreJob>::build(id, builder)
    }
}

#[async_trait::async_trait]
impl<T> JobExecutor for ExecutorForURLFetchAndStoreJob<T>
where
    T: URLFetchAndStoreJobWithData + Send + Sync,
{
    async fn execute(
        &self,
        id: JobId,
        data: Vec<u8>,
        _dependency_data: Option<Vec<u8>>,
        actions: &mut Actions,
    ) -> Result<()> {
        let url = T::get_url(&id, data)?;
        // TODO: Cache client
        let client = reqwest::Client::new();
        let builder = client.get(url);
        let builder = T::build(&id, builder)?;
        let response = builder
            .send()
            .await
            .map_err(|e| Error::JobExecution(Box::new(e)))?;
        let status = response.status();
        if !status.is_success() {
            // TODO: Trace?
            // TODO: Handle 429 errors differently? retryable vs non retryable ones?
            // TODO: Parse to see if the data has an "error"?
            let headers = response.headers().clone();
            let data = response
                .bytes()
                .await
                .map_err(|e| Error::JobExecution(Box::new(e)))?
                .to_vec();
            let text = String::from_utf8_lossy(&data);
            println!(
                "Error fetching data of type {} for {:?} - {:?}: {:?}",
                std::any::type_name::<T>(),
                id,
                headers,
                text
            );
            return Err(Box::new(Error::URLFetchAndStoreJobError(
                std::any::type_name::<T>().to_owned(),
                id,
                text.to_string(),
            )));
        }
        let data = response
            .bytes()
            .await
            .map_err(|e| Error::JobExecution(Box::new(e)))?
            .to_vec();
        actions.write_data((BlobCompressionAlgorithm::Zstd, data, IsAlreadyEncoded::No).into());
        Ok(())
    }
}

// TODO: Code share with the above
#[async_trait::async_trait]
impl<T> JobExecutor for ExecutorForURLPostAndStoreJob<T>
where
    T: URLPostAndStoreJob + Send + Sync,
{
    async fn execute(
        &self,
        id: JobId,
        data: Vec<u8>,
        _dependency_data: Option<Vec<u8>>,
        actions: &mut Actions,
    ) -> Result<()> {
        let url = self.0.get_url(&id, &data)?;
        // TODO: Cache client
        let client = reqwest::Client::new();
        let builder = client.post(url);
        let builder = self.0.build(&id, &data, builder)?;
        let response = builder
            .send()
            .await
            .map_err(|e| Error::JobExecution(Box::new(e)))?;
        let status = response.status();
        if !status.is_success() {
            // TODO: Trace?
            // TODO: Handle 429 errors differently? retryable vs non retryable ones?
            // TODO: Parse to see if the data has an "error"?
            let headers = response.headers().clone();
            let data = response
                .bytes()
                .await
                .map_err(|e| Error::JobExecution(Box::new(e)))?
                .to_vec();
            let text = String::from_utf8_lossy(&data);
            println!(
                "Error fetching data of type {} for {:?} - {:?}: {:?}",
                std::any::type_name::<T>(),
                id,
                headers,
                text
            );
            return Err(Box::new(Error::URLFetchAndStoreJobError(
                std::any::type_name::<T>().to_owned(),
                id,
                text.to_string(),
            )));
        }
        let data = response
            .bytes()
            .await
            .map_err(|e| Error::JobExecution(Box::new(e)))?
            .to_vec();
        actions.write_data((BlobCompressionAlgorithm::Zstd, data, IsAlreadyEncoded::No).into());
        Ok(())
    }
}

#[async_trait::async_trait]
impl<T> JobExecutor for ExecutorForJobWithDependencyData<T>
where
    T: JobWithDependencyData + Send + Sync,
{
    #[inline]
    async fn execute(
        &self,
        id: JobId,
        _data: Vec<u8>,
        dependency_data: Option<Vec<u8>>,
        actions: &mut Actions,
    ) -> Result<()> {
        if let Some(data) = dependency_data {
            self.0.execute(id, data, actions).await
        } else {
            Err(crate::error::Error::NoDependencyDataFoundForJob(
                std::any::type_name::<T>().to_owned(),
            )
            .into())
        }
    }
}

#[async_trait::async_trait]
impl<T> JobExecutor for ExecutorForJobWithSerializedDependencyData<T>
where
    T: JobWithSerializedDependencyData + Send + Sync,
    T::Data: DeserializeOwned + Send + Sync,
{
    #[inline]
    async fn execute(
        &self,
        id: JobId,
        _data: Vec<u8>,
        dependency_data: Option<Vec<u8>>,
        actions: &mut Actions,
    ) -> Result<()> {
        if let Some(data) = dependency_data {
            let deserialized: T::Data = serde_json::from_slice(&data)?;
            self.0.execute(id, deserialized, actions).await
        } else {
            Err(crate::error::Error::NoDependencyDataFoundForJob(
                std::any::type_name::<T>().to_owned(),
            )
            .into())
        }
    }
}
