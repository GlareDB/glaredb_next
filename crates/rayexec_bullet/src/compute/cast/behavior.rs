use rayexec_error::{RayexecError, Result};

use crate::{array::Array, bitmap::Bitmap};

/// Behavior when a cast fail due to under/overflow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CastFailBehavior {
    /// Return an error.
    Error,
    /// Use a NULL value.
    Null,
}

impl CastFailBehavior {
    pub(crate) fn new_state_for_array(&self, arr: &Array) -> CastFailState {
        match self {
            CastFailBehavior::Error => CastFailState::TrackOneAndError(None),
            CastFailBehavior::Null => CastFailState::TrackManyAndInvalidate(Vec::new()),
        }
    }
}

/// State used to track failures casting.
#[derive(Debug)]
pub(crate) enum CastFailState {
    /// Keep the row index of the first failure.
    TrackOneAndError(Option<usize>),
    /// Track all failures during casting.
    TrackManyAndInvalidate(Vec<usize>),
}

impl CastFailState {
    pub(crate) fn set_did_fail(&mut self, idx: usize) {
        match self {
            Self::TrackOneAndError(maybe_idx) => {
                if maybe_idx.is_none() {
                    *maybe_idx = Some(idx);
                }
            }
            Self::TrackManyAndInvalidate(indices) => indices.push(idx),
        }
    }

    pub(crate) fn check_and_apply(self, original: &Array, mut output: Array) -> Result<Array> {
        match self {
            Self::TrackOneAndError(None) => Ok(output),
            Self::TrackOneAndError(Some(idx)) => {
                let scalar = original.logical_value(idx)?;
                Err(RayexecError::new(format!(
                    "Failed to parse '{scalar}' into {}",
                    output.datatype()
                )))
            }
            Self::TrackManyAndInvalidate(indices) => {
                if indices.is_empty() {
                    Ok(output)
                } else {
                    // Apply the nulls.
                    for idx in indices {
                        output.set_physical_validity(idx, false);
                    }
                    Ok(output)
                }
            }
        }
    }
}
