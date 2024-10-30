use rayexec_bullet::array::Array;
use rayexec_bullet::executor::aggregate::RowToStateMapping;

use super::aggregate_hash_table::{Aggregate, AggregateStates};

/// Holds a chunk of value for the aggregate hash table.
#[derive(Debug)]
pub struct GroupChunk {
    /// Number of groups in this chunk.
    pub num_groups: usize,
    /// All row hashes.
    pub hashes: Vec<u64>,
    /// Arrays making up the group values.
    pub arrays: Vec<Array>,
    /// Aggregate states we're keeping track of.
    pub aggregate_states: Vec<AggregateStates>,
}

impl GroupChunk {}
