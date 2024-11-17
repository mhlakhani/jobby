// encodings for a blob
use serde::{Deserialize, Serialize};
use strum_macros::FromRepr;

/// Compression algorithm
#[derive(
    Debug, Clone, Copy, FromRepr, serde_repr::Serialize_repr, serde_repr::Deserialize_repr,
)]
#[repr(i64)]
pub enum BlobCompressionAlgorithm {
    None = 0,
    Zstd,
}

impl Default for BlobCompressionAlgorithm {
    fn default() -> Self {
        Self::None
    }
}

/// Metadata for things like compression and storage
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct BlobMetadata {
    #[serde(rename = "a")]
    pub compression_algorithm: BlobCompressionAlgorithm,
    #[serde(rename = "d")]
    pub dictionary_id: usize,
}

impl BlobMetadata {
    pub const fn no_compression() -> Self {
        Self {
            compression_algorithm: BlobCompressionAlgorithm::None,
            dictionary_id: 0,
        }
    }

    pub const fn with_compression(compression_algorithm: BlobCompressionAlgorithm) -> Self {
        Self {
            compression_algorithm,
            dictionary_id: 0,
        }
    }
}
