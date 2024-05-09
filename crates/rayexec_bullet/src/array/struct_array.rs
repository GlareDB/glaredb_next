use rayexec_error::{RayexecError, Result};

use crate::{bitmap::Bitmap, field::DataType, scalar::ScalarValue};

use super::Array;
use std::sync::Arc;

#[derive(Debug, PartialEq)]
pub struct StructArray {
    validity: Option<Bitmap>,
    arrays: Vec<Arc<Array>>,
    keys: Vec<String>,
}

impl StructArray {
    pub fn try_new(keys: Vec<String>, values: Vec<Arc<Array>>) -> Result<Self> {
        if keys.len() != values.len() {
            return Err(RayexecError::new(format!(
                "Received {} keys for struct, but only {} values",
                keys.len(),
                values.len()
            )));
        }

        Ok(StructArray {
            validity: None,
            arrays: values,
            keys,
        })
    }

    pub fn len(&self) -> usize {
        self.arrays[0].len()
    }

    pub fn is_valid(&self, idx: usize) -> Option<bool> {
        if idx >= self.len() {
            return None;
        }

        Some(super::is_valid(self.validity.as_ref(), idx))
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

    pub fn scalar(&self, idx: usize) -> Option<ScalarValue> {
        if idx >= self.len() {
            return None;
        }

        let scalars: Vec<_> = self
            .arrays
            .iter()
            .map(|arr| arr.scalar(idx).unwrap())
            .collect();

        Some(ScalarValue::Struct(scalars))
    }
}
