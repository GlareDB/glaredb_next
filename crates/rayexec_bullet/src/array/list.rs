use crate::{
    bitmap::Bitmap,
    datatype::{DataType, ListTypeMeta},
    scalar::ScalarValue,
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
        debug_assert_eq!(
            offsets.len() - 1,
            validity
                .as_ref()
                .map(|v| v.len())
                .unwrap_or(offsets.len() - 1)
        );

        let child = child.into();
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

    pub fn scalar(&self, idx: usize) -> Option<ScalarValue> {
        if idx >= self.len() {
            return None;
        }

        let start = self.offsets.as_ref()[idx].as_usize();
        let end = self.offsets.as_ref()[idx + 1].as_usize();

        let mut vals = Vec::with_capacity(end - start);

        for idx in start..end {
            let val = self.child.scalar(idx)?; // TODO: Should probably unwrap here.
            vals.push(val);
        }

        Some(ScalarValue::List(vals))
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

        if self.validity != other.validity {
            return false;
        }

        self.child == other.child
    }
}
