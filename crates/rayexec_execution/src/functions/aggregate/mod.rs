pub mod numeric;

use dyn_clone::DynClone;
use once_cell::sync::Lazy;
use rayexec_bullet::{array::Array, executor::aggregate::AggregateState, field::DataType};
use rayexec_error::{RayexecError, Result};
use std::any::Any;
use std::{
    fmt::{self, Debug},
    marker::PhantomData,
};

use super::{ReturnType, Signature};

pub static ALL_AGGREGATE_FUNCTIONS: Lazy<Vec<Box<dyn GenericAggregateFunction>>> =
    Lazy::new(|| vec![Box::new(numeric::Sum)]);

/// A generic aggregate function that can be specialized into a more specific
/// function depending on type.
pub trait GenericAggregateFunction: Debug + Sync + Send + DynClone {
    /// Name of the function.
    fn name(&self) -> &str;

    /// Optional aliases for this function.
    fn aliases(&self) -> &[&str] {
        &[]
    }

    fn signatures(&self) -> &[Signature];

    fn return_type_for_inputs(&self, inputs: &[DataType]) -> Option<DataType> {
        let sig = self
            .signatures()
            .iter()
            .find(|sig| sig.inputs_satisfy_signature(inputs))?;

        match &sig.return_type {
            ReturnType::Static(datatype) => Some(datatype.clone()),
            ReturnType::Dynamic => None,
        }
    }

    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedAggregateFunction>>;
}

impl Clone for Box<dyn GenericAggregateFunction> {
    fn clone(&self) -> Self {
        dyn_clone::clone_box(&**self)
    }
}

pub trait SpecializedAggregateFunction: Debug + Sync + Send + DynClone {
    fn new_grouped_state(&self) -> Box<dyn GroupedStates>;
}

pub trait GroupedStates: Debug + Send {
    /// Needed to allow downcasting to the concrete type when combining multiple
    /// states that were computed in parallel.
    fn as_any_mut(&mut self) -> &mut dyn Any;

    /// Generate a new state for a never before seen group in an aggregate.
    ///
    /// Returns the index of the newly initialized state that can be used to
    /// reference the state.
    fn new_group(&mut self) -> usize;

    /// Get the number of group states we're tracking.
    fn num_groups(&self) -> usize;

    /// Updates states for groups using the provided inputs.
    ///
    /// Each row in `inputs` corresponds to values that should be used to update
    /// states.
    ///
    /// `mapping` provides a mapping from the input row to the state that should
    /// be updated. The 'n'th row in the input corresponds to the 'n'th value in
    /// `mapping` which corresponds to the state to be updated with the 'n'th
    /// row.
    fn update_from_arrays(&mut self, inputs: &[&Array], mapping: &[usize]) -> Result<()>;

    /// Try to combine two sets of grouped states into a single set of states.
    ///
    /// Errors if the concrete types do not match. Essentially this prevents
    /// trying to combine state between different aggregates (SumI32 and AvgF32)
    /// _and_ type (SumI32 and SumI64).
    // TODO: Mapping
    fn try_combine(&mut self, consume: Box<dyn GroupedStates>, mapping: &[usize]) -> Result<()>;

    /// Finalizes the aggregate states into a single array.
    fn finalize(&mut self) -> Result<Array>;
}

/// Provides a default implementation of `GroupedStates`.
///
/// Since we're working with multiple aggregates at a time, we need to be able
/// to box `GroupedStates`, and this type just enables doing that easily.
///
/// This essetially provides a wrapping around functions provided by the
/// aggregate executors, and some number of aggregate states.
pub struct DefaultGroupedStates<S, T, O, UF, CF, FF> {
    /// All states we're tracking.
    ///
    /// Each state corresponds to a single group.
    states: Vec<S>,

    /// How we should update states given inputs and a mapping array.
    update_fn: UF,

    /// How we should combine states.
    combine_fn: CF,

    /// How we should finalize the states once we're done updating states.
    finalize_fn: FF,

    _t: PhantomData<T>,
    _o: PhantomData<O>,
}

impl<S, T, O, UF, CF, FF> DefaultGroupedStates<S, T, O, UF, CF, FF>
where
    S: AggregateState<T, O>,
    UF: Fn(&[&Array], &[usize], &mut [S]) -> Result<()>,
    CF: Fn(Vec<S>, &[usize], &mut [S]) -> Result<()>,
    FF: Fn(Vec<S>) -> Result<Array>,
{
    fn new(update_fn: UF, combine_fn: CF, finalize_fn: FF) -> Self {
        DefaultGroupedStates {
            states: Vec::new(),
            update_fn,
            combine_fn,
            finalize_fn,
            _t: PhantomData,
            _o: PhantomData,
        }
    }
}

impl<S, T, O, UF, CF, FF> GroupedStates for DefaultGroupedStates<S, T, O, UF, CF, FF>
where
    T: Send + 'static,
    O: Send + 'static,
    S: AggregateState<T, O> + Send + 'static,
    UF: Fn(&[&Array], &[usize], &mut [S]) -> Result<()> + Send + 'static,
    CF: Fn(Vec<S>, &[usize], &mut [S]) -> Result<()> + Send + 'static,
    FF: Fn(Vec<S>) -> Result<Array> + Send + 'static,
{
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn new_group(&mut self) -> usize {
        let idx = self.states.len();
        self.states.push(S::default());
        idx
    }

    fn num_groups(&self) -> usize {
        self.states.len()
    }

    fn update_from_arrays(&mut self, inputs: &[&Array], mapping: &[usize]) -> Result<()> {
        (self.update_fn)(inputs, mapping, &mut self.states)
    }

    fn try_combine(
        &mut self,
        mut consume: Box<dyn GroupedStates>,
        mapping: &[usize],
    ) -> Result<()> {
        let other = match consume.as_any_mut().downcast_mut::<Self>() {
            Some(other) => other,
            None => {
                return Err(RayexecError::new(
                    "Attempted to combine aggregate states of different types",
                ))
            }
        };

        let consume = std::mem::take(&mut other.states);
        (self.combine_fn)(consume, mapping, &mut self.states)
    }

    fn finalize(&mut self) -> Result<Array> {
        (self.finalize_fn)(std::mem::take(&mut self.states))
    }
}

impl<S, T, O, UF, CF, FF> Debug for DefaultGroupedStates<S, T, O, UF, CF, FF>
where
    S: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DefaultGroupedStates")
            .field("states", &self.states)
            .finish_non_exhaustive()
    }
}
