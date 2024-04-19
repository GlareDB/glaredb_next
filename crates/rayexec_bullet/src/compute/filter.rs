use crate::array::BooleanArray;
use rayexec_error::Result;

pub trait FilterKernel: Sized {
    /// Filter self using a selection array.
    fn filter(&self, selection: &BooleanArray) -> Result<Self>;
}
