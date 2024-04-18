use std::borrow::BorrowMut;

/// An LSB ordered bitmap.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Bitmap {
    len: usize,
    data: Vec<u8>,
}

impl Bitmap {
    pub fn from_bool_iter(iter: impl IntoIterator<Item = bool>) -> Self {
        let mut iter = iter.into_iter();

        let mut data = Vec::new();
        let mut len = 0;

        loop {
            let mut byte = 0;
            let mut bit_len = 0;

            for (idx, bit) in iter.borrow_mut().take(8).enumerate() {
                bit_len += 1;
                if bit {
                    byte = byte | (1 << idx);
                }
            }

            // No more bits, exit loop.
            if bit_len == 0 {
                break;
            }

            // Push byte, continue loop to get next 8 values.
            data.push(byte);
            len += bit_len;
        }

        Bitmap { len, data }
    }

    pub const fn len(&self) -> usize {
        self.len
    }

    /// Get the value at index.
    ///
    /// Panics if index is out of bounds.
    pub fn value(&self, idx: usize) -> bool {
        assert!(idx < self.len);
        self.data[idx / 8] & (1 << (idx % 8)) != 0
    }

    /// Set a bit at index.
    pub fn set(&mut self, idx: usize, val: bool) {
        assert!(idx < self.len);
        if val {
            // Set bit.
            self.data[idx / 8] = self.data[idx / 8] | (1 << (idx % 8))
        } else {
            // Unset bit
            self.data[idx / 8] = self.data[idx / 8] & !(1 << (idx % 8))
        }
    }

    /// Get an iterator over the bitmap.
    pub const fn iter(&self) -> BitmapIter {
        BitmapIter {
            idx: 0,
            bitmap: self,
        }
    }
}

#[derive(Debug)]
pub struct BitmapIter<'a> {
    idx: usize,
    bitmap: &'a Bitmap,
}

impl<'a> Iterator for BitmapIter<'a> {
    type Item = bool;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.bitmap.len() {
            return None;
        }

        let v = self.bitmap.value(self.idx);
        self.idx += 1;
        Some(v)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.bitmap.len() - self.idx,
            Some(self.bitmap.len() - self.idx),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple() {
        let bits = [true, false, true, false, true, true, true, true];
        let bm = Bitmap::from_bool_iter(bits);

        assert_eq!(8, bm.len());

        let got: Vec<_> = bm.iter().collect();
        assert_eq!(bits.as_slice(), got);
    }

    #[test]
    fn simple_multiple_bytes() {
        let bits = [
            true, false, true, false, true, true, true, true, //
            true, false, true, false, false, true, true, true, //
            true, false, true, false, true, false, true, true,
        ];
        let bm = Bitmap::from_bool_iter(bits);

        assert_eq!(24, bm.len());

        let got: Vec<_> = bm.iter().collect();
        assert_eq!(bits.as_slice(), got);
    }

    #[test]
    fn not_multiple_of_eight() {
        let bits = [
            true, false, true, false, true, true, true, true, //
            true, false, true, false,
        ];
        let bm = Bitmap::from_bool_iter(bits);

        assert_eq!(12, bm.len());

        let got: Vec<_> = bm.iter().collect();
        assert_eq!(bits.as_slice(), got);
    }

    #[test]
    fn set_simple() {
        let bits = [true, false, true, false, true, true, true, true];
        let mut bm = Bitmap::from_bool_iter(bits);

        bm.set(0, false);
        assert_eq!(false, bm.value(0));

        bm.set(1, true);
        assert_eq!(true, bm.value(1));
    }
}
