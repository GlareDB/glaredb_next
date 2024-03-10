use arrow_array::{Array, ArrayRef};
use rayexec_error::{RayexecError, Result};
use std::fmt::Debug;

pub trait AccumulatorState: Send + Sync + Debug {
    /// Number of groups represented by this state.
    fn num_groups(&self) -> usize;
}

/// Downcast this to the concrete state to allow for merging states across
/// multiple instances of an accumulator.
pub fn downcast_state_mut<T: AccumulatorState + 'static>(
    state: &mut Box<dyn AccumulatorState>,
) -> Result<&mut T> {
    let s: &mut dyn std::any::Any = state;
    s.downcast_mut::<T>()
        .ok_or_else(|| RayexecError::new("failed to downcast to requested state"))
}

pub trait Accumulator: Sync + Send + Debug {
    /// Update the internal state for the group at the given index using `vals`.
    ///
    /// This may be called out of order, and th accumulator should initialized
    /// skipped groups to some uninitialized state.
    fn accumulate(&mut self, group_idx: usize, vals: &[&ArrayRef]) -> Result<()>;

    /// Take the internal state so that it can be merged with another instance
    /// of this accumulator.
    fn take_state(&mut self) -> Result<Box<dyn AccumulatorState>>;

    /// Update the internal state using the state from a different instances.
    ///
    /// `groups` provides the mapping from the external state to internal state.
    /// The group index at `groups[0]` corresponds to the index of the internal
    /// state.
    fn update_from_state(
        &mut self,
        groups: &[usize],
        state: Box<dyn AccumulatorState>,
    ) -> Result<()>;

    /// Produce the final result of accumulation.
    fn finish(&mut self) -> Result<ArrayRef>;
}
