use crate::{bitmap::Bitmap, field::DataType};

use super::Array;
use std::sync::Arc;

#[derive(Debug, PartialEq)]
pub struct StructArray {
    validity: Option<Bitmap>,
    arrays: Vec<Arc<Array>>,
    keys: Vec<String>,
}

impl StructArray {
    pub fn len(&self) -> usize {
        self.arrays[0].len()
    }

    pub fn datatype(&self) -> DataType {
        let fields = self
            .keys
            .iter()
            .zip(self.arrays.iter())
            .map(|(_key, arr)| arr.datatype())
            .collect();
        DataType::Struct { fields }
    }

    pub fn array_for_key(&self, key: &str) -> Option<&Arc<Array>> {
        let idx = self.keys.iter().position(|k| *k == key)?;
        self.arrays.get(idx)
    }
}
