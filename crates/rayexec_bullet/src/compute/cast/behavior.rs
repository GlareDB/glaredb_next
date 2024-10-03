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
            CastFailBehavior::Null => {
                CastFailState::TrackManyAndInvalidate(arr.logical_len(), None)
            }
        }
    }
}

/// State used to track failures casting.
#[derive(Debug)]
pub(crate) enum CastFailState {
    /// Keep the row index of the first failure.
    TrackOneAndError(Option<usize>),
    /// Track all failures during casting.
    ///
    /// Lazily allocates the validity bitmap. ANDed with the resulting validity
    /// bitmap for the array.
    TrackManyAndInvalidate(usize, Option<Bitmap>),
}

impl CastFailState {
    pub(crate) fn set_did_fail(&mut self, idx: usize) {
        match self {
            Self::TrackOneAndError(maybe_idx) => {
                if maybe_idx.is_none() {
                    *maybe_idx = Some(idx);
                }
            }
            Self::TrackManyAndInvalidate(len, maybe_bitmap) => {
                let bitmap = maybe_bitmap.get_or_insert_with(|| Bitmap::new_with_all_true(*len));
                bitmap.set_unchecked(idx, false);
            }
        }
    }

    pub(crate) fn apply(&self, arr: Array) -> Result<Array> {
        match self {
            Self::TrackOneAndError(None) => Ok(arr),
            Self::TrackOneAndError(Some(idx)) => {
                let scalar = arr.logical_value(*idx)?;
                Err(RayexecError::new(format!(
                    "Failed to parse '{scalar}' into {}",
                    arr.datatype()
                )))
            }
            _ => unimplemented!(), // TODO
        }
    }
}
