use rayexec_bullet::array::Array;
use rayexec_error::Result;

use super::aggregate_hash_table::AggregateStates;
use super::hash_table::GroupAddress;
use crate::functions::aggregate::ChunkGroupAddressIter;

/// Holds a chunk of value for the aggregate hash table.
#[derive(Debug)]
pub struct GroupChunk {
    /// Index of this chunk.
    pub chunk_idx: u32,
    /// Number of groups in this chunk.
    pub num_groups: usize,
    /// All row hashes.
    pub hashes: Vec<u64>,
    /// Arrays making up the group values.
    pub arrays: Vec<Array>,
    /// Aggregate states we're keeping track of.
    pub aggregate_states: Vec<AggregateStates>,
}

impl GroupChunk {
    /// Update all states in this chunk using rows in `inputs`.
    ///
    /// `addrs` contains a list of group addresses we'll be using to map input
    /// rows to the state index. If and address is for a different chunk, that
    /// row will be skipped.
    pub fn update_states(&mut self, inputs: &[Array], addrs: &[GroupAddress]) -> Result<()> {
        for agg_states in &mut self.aggregate_states {
            let input_cols: Vec<_> = agg_states
                .col_selection
                .iter()
                .zip(inputs.iter())
                .filter_map(|(selected, arr)| if selected { Some(arr) } else { None })
                .collect();

            agg_states.states.update_states(
                &input_cols,
                ChunkGroupAddressIter::new(self.chunk_idx, addrs),
            )?;
        }

        Ok(())
    }

    /// Merges other into self according to `addrs`.
    ///
    /// Only addresses with this chunk's idx will be used, and the `row_idx` in
    /// the address corresponds to the aggregate row state in self to update
    /// from that addressess' position.
    pub fn combine_states(&mut self, other: &mut GroupChunk, addrs: &[GroupAddress]) -> Result<()> {
        for agg_idx in 0..self.aggregate_states.len() {
            let own_state = &mut self.aggregate_states[agg_idx];
            let other_state = &mut other.aggregate_states[agg_idx];

            own_state.states.combine(
                &mut other_state.states,
                ChunkGroupAddressIter::new(self.chunk_idx, addrs),
            )?;
        }

        Ok(())
    }
}
