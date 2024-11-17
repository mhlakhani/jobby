use crate::{
    blob_encoding::{BlobCompressionAlgorithm, BlobMetadata},
    Error,
};

type Result<T, E = crate::Error> = anyhow::Result<T, E>;

// blobs

#[derive(Debug)]
pub enum IsAlreadyEncoded {
    Yes,
    No,
}

#[derive(Debug)]
enum EncodedOrRawData {
    Encoded(Vec<u8>),
    Raw(Vec<u8>),
}

impl From<(IsAlreadyEncoded, Vec<u8>)> for EncodedOrRawData {
    fn from((encoded, data): (IsAlreadyEncoded, Vec<u8>)) -> Self {
        match encoded {
            IsAlreadyEncoded::Yes => Self::Encoded(data),
            IsAlreadyEncoded::No => Self::Raw(data),
        }
    }
}

// Wrapper type so we can submit/receive decoded data easily
#[derive(Debug)]
pub struct JobbyBlob {
    // This is the intended metadata for it to be stored with
    metadata: BlobMetadata,
    data: EncodedOrRawData,
}

impl JobbyBlob {
    // Will spawn work in the background for expensive operations
    pub(crate) async fn encode(self) -> Result<Self> {
        // TODO: thread local compressors, dictionaries
        match self.data {
            EncodedOrRawData::Encoded(data) => Ok(Self {
                metadata: self.metadata,
                data: EncodedOrRawData::Encoded(data),
            }),
            EncodedOrRawData::Raw(data) => match self.metadata.compression_algorithm {
                BlobCompressionAlgorithm::None => Ok(Self {
                    metadata: self.metadata,
                    data: EncodedOrRawData::Encoded(data),
                }),
                BlobCompressionAlgorithm::Zstd => {
                    let data = tokio::task::spawn_blocking(move || {
                        zstd::stream::encode_all(data.as_slice(), 0)
                    })
                    .await??;
                    Ok(Self {
                        metadata: self.metadata,
                        data: EncodedOrRawData::Encoded(data),
                    })
                }
            },
        }
    }

    pub(crate) async fn decode(self) -> Result<Self> {
        // TODO: thread local compressors, dictionaries
        match self.data {
            EncodedOrRawData::Encoded(data) => match self.metadata.compression_algorithm {
                BlobCompressionAlgorithm::None => Ok(Self {
                    metadata: self.metadata,
                    data: EncodedOrRawData::Raw(data),
                }),
                BlobCompressionAlgorithm::Zstd => {
                    let data = tokio::task::spawn_blocking(move || {
                        zstd::stream::decode_all(data.as_slice())
                    })
                    .await??;
                    Ok(Self {
                        metadata: self.metadata,
                        data: EncodedOrRawData::Raw(data),
                    })
                }
            },
            EncodedOrRawData::Raw(data) => Ok(Self {
                metadata: self.metadata,
                data: EncodedOrRawData::Raw(data),
            }),
        }
    }

    pub(crate) fn take(self) -> Result<Vec<u8>> {
        match self.data {
            EncodedOrRawData::Encoded(_) => Err(Error::JobbyBlobUnexpectedEncodedData),
            EncodedOrRawData::Raw(data) => Ok(data),
        }
    }

    pub(crate) fn into_parts(self) -> (BlobMetadata, Vec<u8>) {
        match self.data {
            EncodedOrRawData::Encoded(data) | EncodedOrRawData::Raw(data) => (self.metadata, data),
        }
    }

    // Used when a job has no data, since we need to insert *something*
    pub(crate) const fn empty_parts() -> (BlobMetadata, Vec<u8>) {
        (BlobMetadata::no_compression(), vec![])
    }
}

impl From<()> for JobbyBlob {
    fn from((): ()) -> Self {
        Self {
            metadata: BlobMetadata::no_compression(),
            data: EncodedOrRawData::Raw(vec![]),
        }
    }
}

impl From<Vec<u8>> for JobbyBlob {
    fn from(data: Vec<u8>) -> Self {
        Self {
            metadata: BlobMetadata::no_compression(),
            data: EncodedOrRawData::Raw(data),
        }
    }
}

impl From<String> for JobbyBlob {
    fn from(data: String) -> Self {
        Self {
            metadata: BlobMetadata::no_compression(),
            data: EncodedOrRawData::Raw(data.into_bytes()),
        }
    }
}

impl From<(BlobMetadata, Vec<u8>, IsAlreadyEncoded)> for JobbyBlob {
    fn from((metadata, data, encoded): (BlobMetadata, Vec<u8>, IsAlreadyEncoded)) -> Self {
        Self {
            metadata,
            data: (encoded, data).into(),
        }
    }
}

impl From<(BlobCompressionAlgorithm, Vec<u8>, IsAlreadyEncoded)> for JobbyBlob {
    fn from(
        (algorithm, data, encoded): (BlobCompressionAlgorithm, Vec<u8>, IsAlreadyEncoded),
    ) -> Self {
        Self {
            metadata: BlobMetadata::with_compression(algorithm),
            data: (encoded, data).into(),
        }
    }
}
