//! In-memory storage formats.

mod primitive;
pub use primitive::*;

mod varlen;
pub use varlen::*;

mod shared_heap;
pub use shared_heap::*;

pub trait AddressableStorage {
    type T: ?Sized;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn get(&self, idx: usize) -> Option<&Self::T>;

    unsafe fn get_unchecked(&self, idx: usize) -> &Self::T;
}
