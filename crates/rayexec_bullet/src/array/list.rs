use crate::{
    bitmap::Bitmap,
    datatype::{DataType, ListTypeMeta},
    storage::PrimitiveStorage,
};
use std::sync::Arc;

use super::{Array, OffsetIndex};

#[derive(Debug)]
pub struct VariableListArray<O: OffsetIndex> {
    /// Value validities.
    validity: Option<Bitmap>,

    /// Offsets into the child array.
    ///
    /// Length should be one more than the number of values being held in this
    /// array.
    offsets: PrimitiveStorage<O>,

    /// Child array containing the actual data.
    child: Arc<Array>,
}

pub type ListArray = VariableListArray<i32>;

impl<O> VariableListArray<O>
where
    O: OffsetIndex,
{
    pub fn new(child: impl Into<Arc<Array>>, offsets: Vec<O>, validity: Option<Bitmap>) -> Self {
        let child = child.into();
        debug_assert_eq!(
            child.len(),
            validity.as_ref().map(|v| v.len()).unwrap_or(child.len())
        );
        debug_assert_eq!(child.len() + 1, offsets.len());

        VariableListArray {
            validity,
            offsets: offsets.into(),
            child,
        }
    }

    pub fn data_type(&self) -> DataType {
        DataType::List(ListTypeMeta {
            datatype: Box::new(self.child.datatype()),
        })
    }

    pub fn is_valid(&self, idx: usize) -> Option<bool> {
        if idx >= self.len() {
            return None;
        }

        Some(self.validity.as_ref().map(|v| v.value(idx)).unwrap_or(true))
    }

    pub fn len(&self) -> usize {
        self.offsets.as_ref().len() - 1
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }
}

impl<O> PartialEq for VariableListArray<O>
where
    O: OffsetIndex,
{
    fn eq(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }

        unimplemented!()
        // let left = self.values_iter();
        // let right = other.values_iter();

        // left.zip(right).all(|(left, right)| left == right)
    }
}
