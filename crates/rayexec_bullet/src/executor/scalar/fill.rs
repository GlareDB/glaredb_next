use rayexec_error::Result;

use crate::{
    array::Array,
    bitmap::Bitmap,
    executor::builder::{ArrayBuilder, ArrayDataBuffer},
};

/// Incrementally put values into a new array buffer from existing arrays using
/// selection vectors.
#[derive(Debug)]
pub struct FillState<B: ArrayDataBuffer> {
    pub validity: Bitmap,
    pub builder: ArrayBuilder<B>,
}

impl<B> FillState<B>
where
    B: ArrayDataBuffer,
{
    pub fn fill<'a, S>(&mut self, array: &'a Array) -> Result<()> {
        unimplemented!()
    }
}
