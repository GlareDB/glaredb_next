//! Vectorized aggregate executors.

mod unary;
pub use unary::*;

mod binary;
pub use binary::*;

use rayexec_error::Result;
use std::fmt::Debug;

use crate::{
    array::{validity::union_validities, ArrayAccessor, ValuesBuffer},
    bitmap::{zip::ZipBitmapsIter, Bitmap},
};

/// State for a single group's aggregate.
///
/// An example state for SUM would be a struct that takes a running sum from
/// values provided in `update`.
pub trait AggregateState<Input, Output>: Default + Debug {
    /// Merge other state into this state.
    fn merge(&mut self, other: Self) -> Result<()>;

    /// Update this state with some input.
    fn update(&mut self, input: Input) -> Result<()>;

    /// Produce a single value from the state, along with a bool indicating if
    /// the value is valid.
    fn finalize(self) -> Result<(Output, bool)>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowToStateMapping {
    /// Index of the row we're getting a value from.
    pub from_row: usize,
    /// The index of the state that we'll be updating with the value.
    pub to_state: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct StateCombiner;

impl StateCombiner {
    /// Combine states, merging states from `consume` into `targets`.
    ///
    /// `mapping` provides a mapping of consume states to the target index. The
    /// 'n'th state in `consume` corresponds to the 'n'th value `mapping`. With the value
    /// in mapping being the index of the target state.
    pub fn combine<State, Input, Output>(
        consume: Vec<State>,
        mapping: &[usize],
        targets: &mut [State],
    ) -> Result<()>
    where
        State: AggregateState<Input, Output>,
    {
        for (target_idx, consume_state) in mapping.iter().zip(consume.into_iter()) {
            let target = &mut targets[*target_idx];
            target.merge(consume_state)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StateFinalizer;

impl StateFinalizer {
    /// Finalizes aggregate states, pushing values and validities into the
    /// provided buffers.
    pub fn finalize<S, T, O>(
        states: impl IntoIterator<Item = S>,
        values: &mut impl ValuesBuffer<O>,
        validities: &mut Bitmap,
    ) -> Result<()>
    where
        S: AggregateState<T, O>,
    {
        for state in states {
            let (out, valid) = state.finalize()?;
            values.push_value(out);
            validities.push(valid);
        }

        Ok(())
    }
}
