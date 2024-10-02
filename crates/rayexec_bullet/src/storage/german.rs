use rayexec_error::Result;

use super::{AddressableStorage, PrimitiveStorage};

const INLINE_THRESHOLD: i32 = 12;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GermanVarlenStorage {
    pub(crate) lens: PrimitiveStorage<i32>,
    pub(crate) inline_or_metadata: PrimitiveStorage<[u8; 12]>,
    pub(crate) data: PrimitiveStorage<u8>,
}

impl GermanVarlenStorage {
    pub fn with_lens_and_inline_capacity(len_cap: usize, inline_cap: usize) -> Self {
        GermanVarlenStorage {
            lens: Vec::with_capacity(len_cap).into(),
            inline_or_metadata: Vec::with_capacity(inline_cap).into(),
            data: Vec::new().into(),
        }
    }

    pub fn len(&self) -> usize {
        self.lens.as_ref().len()
    }

    pub fn try_push(&mut self, value: &[u8]) -> Result<()> {
        let lens = self.lens.try_as_vec_mut()?;
        let inline_or_metadata = self.inline_or_metadata.try_as_vec_mut()?;
        let data = self.data.try_as_vec_mut()?;

        if value.len() as i32 <= INLINE_THRESHOLD {
            // Store completely inline.
            lens.push(value.len() as i32);
            let mut inline = [0; 12];
            inline[0..value.len()].copy_from_slice(value);

            inline_or_metadata.push(inline);
        } else {
            // Store prefix, buf index, and offset in line. Store complete copy
            // in buffer.
            lens.push(value.len() as i32);
            let mut metadata = [0; 12];

            // Prefix, 4 bytes
            let prefix_len = std::cmp::min(value.len(), 4);
            metadata[0..prefix_len].copy_from_slice(&value[0..prefix_len]);

            // Buffer index, currently always zero.

            // Offset, 4 bytes
            let offset = data.len();
            data.extend_from_slice(value);
            metadata[9..].copy_from_slice(&(offset as i32).to_le_bytes());

            inline_or_metadata.push(metadata);
        }

        Ok(())
    }

    pub fn get(&self, idx: usize) -> Option<&[u8]> {
        let len = *self.lens.as_ref().get(idx)?;

        if len <= INLINE_THRESHOLD {
            // Read from inline
            let inline = self.inline_or_metadata.as_ref().get(idx)?;
            Some(&inline[..(len as usize)])
        } else {
            // Read from buffer
            let offset = i32::from_le_bytes(
                self.inline_or_metadata.as_ref().get(idx)?[9..]
                    .try_into()
                    .unwrap(),
            );

            Some(&self.data.as_ref()[(offset as usize)..((offset + len) as usize)])
        }
    }

    pub fn iter(&self) -> GermanVarlenIter {
        GermanVarlenIter {
            storage: self,
            idx: 0,
        }
    }

    pub fn as_german_storage_slice(&self) -> GermanVarlenStorageSlice {
        GermanVarlenStorageSlice {
            lens: self.lens.as_ref(),
            inline_or_metadata: self.inline_or_metadata.as_ref(),
            data: self.data.as_ref(),
        }
    }
}

#[derive(Debug)]
pub struct GermanVarlenIter<'a> {
    storage: &'a GermanVarlenStorage,
    idx: usize,
}

impl<'a> Iterator for GermanVarlenIter<'a> {
    type Item = &'a [u8];

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let v = self.storage.get(self.idx)?;
        self.idx += 1;
        Some(v)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.storage.len() - self.idx;
        (remaining, Some(remaining))
    }
}

impl<'a> ExactSizeIterator for GermanVarlenIter<'a> {}

#[derive(Debug)]
pub struct GermanVarlenStorageSlice<'a> {
    lens: &'a [i32],
    inline_or_metadata: &'a [[u8; 12]],
    data: &'a [u8],
}

impl<'a> AddressableStorage for GermanVarlenStorageSlice<'a> {
    type T = [u8];

    fn len(&self) -> usize {
        self.lens.len()
    }

    fn get(&self, idx: usize) -> Option<&Self::T> {
        let len = *self.lens.get(idx)?;

        if len <= INLINE_THRESHOLD {
            // Read from inline
            let inline = self.inline_or_metadata.get(idx)?;
            Some(&inline[..(len as usize)])
        } else {
            // Read from buffer
            let offset =
                i32::from_le_bytes(self.inline_or_metadata.get(idx)?[9..].try_into().unwrap());

            Some(&self.data.as_ref()[(offset as usize)..((offset + len) as usize)])
        }
    }

    unsafe fn get_unchecked(&self, idx: usize) -> &Self::T {
        self.get(idx).unwrap()
    }
}
