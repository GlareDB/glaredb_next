use dyn_clone::DynClone;
use std::fmt::Debug;

/// A generic aggregate function that can be specialized into a more specific
/// function depending on type.
pub trait GenericAggregateFunction: Debug + Sync + Send + DynClone {
    /// Name of the function.
    fn name(&self) -> &str;

    /// Optional aliases for this function.
    fn aliases(&self) -> &[&str] {
        &[]
    }
}

impl Clone for Box<dyn GenericAggregateFunction> {
    fn clone(&self) -> Self {
        dyn_clone::clone_box(&**self)
    }
}
