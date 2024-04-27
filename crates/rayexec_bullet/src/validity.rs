use crate::bitmap::Bitmap;
use rayexec_error::Result;

/// Validity bitmap.
///
/// The underlying bitmap can be omitted if an array isn't nullable. In such a
/// case, every value is considered valid.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Validity(pub(crate) Option<Bitmap>);

impl Validity {
    pub fn invalid_count(&self) -> usize {
        match &self.0 {
            Some(bitmap) => bitmap.len() - bitmap.popcnt(),
            None => todo!(),
        }
    }

    pub fn valid_count(&self) -> usize {
        match &self.0 {
            Some(bitmap) => bitmap.popcnt(),
            None => todo!(),
        }
    }

    pub fn is_valid(&self, idx: usize) -> bool {
        match &self.0 {
            Some(b) => b.value(idx),
            None => true,
        }
    }

    /// Union this validity bitmap with some other validity bitmap.
    pub fn union_mut(&mut self, other: &Validity) -> Result<()> {
        match (&mut self.0, &other.0) {
            // Both validities have bitmaps we should take into account.
            (Some(s), Some(other)) => s.bit_and_mut(other)?,
            // Nothing to do since `other` is all valid.
            (Some(_), None) => (),
            // Self is all valid, so we just need to clone in other.
            (s @ None, Some(other)) => *s = Some(other.clone()),
            // Both bitmaps are all valid.
            (None, None) => (),
        }
        Ok(())
    }
}
