use cluster::ClusterError;
use handle::IndexHandle;
use settings::Settings;
use tantivy::Index;
use uuid::Uuid;

/// Trait implemented by both Primary and Replica Shards
pub trait Shard {
    fn shard_id(&self) -> Uuid;
    fn primary_shard_id(&self) -> Option<Uuid>;
    fn is_primary(&self) -> bool;
    fn index_name(&self) -> Result<String, ClusterError>;
}

/// A PrimaryShard is a writable partition of an Index
#[derive(Serialize, Deserialize)]
pub struct PrimaryShard {
    shard_id: Uuid,
    #[serde(skip_serializing, skip_deserializing)]
    index_handle: Option<IndexHandle>,
}

/// A ReplicaShard is a copy of a specific PrimaryShard that is read-only
#[derive(Serialize, Deserialize)]
pub struct ReplicaShard {
    shard_id: Uuid,
    primary_shard_id: Uuid,
    #[serde(skip_serializing, skip_deserializing)]
    index_handle: Option<IndexHandle>,
}

impl PrimaryShard {
    /// Creates and returns a new PrimaryShard with a random ID
    pub fn new() -> PrimaryShard {
        PrimaryShard::default()
    }

    /// Adds an IndexHandle to a PrimaryShard
    pub fn with_index(mut self, index: Index, name: String) -> Result<PrimaryShard, ClusterError> {
        let settings = Settings::default();
        match IndexHandle::new(index, settings, &name) {
            Ok(lh) => {
                self.index_handle = Some(lh);
                Ok(self)
            }
            Err(e) => Err(ClusterError::FailedCreatingPrimaryShard(e.to_string())),
        }
    }
}

impl Default for PrimaryShard {
    fn default() -> Self {
        PrimaryShard {
            shard_id: Uuid::new_v4(),
            index_handle: None,
        }
    }
}

impl Shard for PrimaryShard {
    /// Returns the UUID for this shard
    fn shard_id(&self) -> Uuid {
        self.shard_id
    }

    /// Since this is not a Replica Shard, return None
    fn primary_shard_id(&self) -> Option<Uuid> {
        None
    }

    /// Simple function to check if a shard is a Primary
    fn is_primary(&self) -> bool {
        true
    }

    /// Returns the name from the underlying IndexHandle
    fn index_name(&self) -> Result<String, ClusterError> {
        match self.index_handle {
            Some(ref handle) => Ok(handle.name()),
            None => Err(ClusterError::UnableToGetIndexHandle),
        }
    }
}
impl ReplicaShard {
    /// Creates and returns a new ReplicaShard that will be a read-only copy of a PrimaryShard
    pub fn new(primary_shard_id: Uuid) -> ReplicaShard {
        ReplicaShard {
            primary_shard_id,
            shard_id: Uuid::new_v4(),
            index_handle: None,
        }
    }

    /// Adds an IndexHandle to a ReplicaShard
    pub fn with_index(mut self, index: Index, name: String) -> Result<ReplicaShard, ClusterError> {
        let settings = Settings::default();
        match IndexHandle::new(index, settings, &name) {
            Ok(lh) => {
                self.index_handle = Some(lh);
                Ok(self)
            }
            Err(e) => Err(ClusterError::FailedCreatingPrimaryShard(e.to_string())),
        }
    }
}

impl Shard for ReplicaShard {
    /// Returns the UUID for this shard
    fn shard_id(&self) -> Uuid {
        self.shard_id
    }

    /// Since this is a replica shard, returns the ID of the shard it is
    /// a replica of
    fn primary_shard_id(&self) -> Option<Uuid> {
        Some(self.primary_shard_id)
    }

    /// Simple function to check if it is a primary shard
    fn is_primary(&self) -> bool {
        false
    }

    /// Returns the name of the underlying Index
    fn index_name(&self) -> Result<String, ClusterError> {
        match self.index_handle {
            Some(ref handle) => Ok(handle.name()),
            None => Err(ClusterError::UnableToGetIndexHandle),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_primary_shard() {
        let test_shard = PrimaryShard::new();
        assert!(test_shard.is_primary());
    }

    #[test]
    fn test_create_replica_shard() {
        let test_primary_shard = PrimaryShard::new();
        let test_replica_shard = ReplicaShard::new(test_primary_shard.shard_id());
        assert!(!test_replica_shard.is_primary());
    }
}
