//! In-memory storage formats.

mod primitive;
pub use primitive::*;

mod varlen;
pub use varlen::*;

mod shared_heap;
pub use shared_heap::*;

mod german;
pub use german::*;

mod boolean;
pub use boolean::*;

mod untyped_null;
pub use untyped_null::*;

use std::fmt::Debug;

/// In-memory array storage that can be directly indexed into.
pub trait AddressableStorage: Debug {
    /// The type we can get from the storage.
    type T: Debug;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn get(&self, idx: usize) -> Option<Self::T>;

    unsafe fn get_unchecked(&self, idx: usize) -> Self::T;
}
