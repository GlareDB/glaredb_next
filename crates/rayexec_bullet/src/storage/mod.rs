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

use std::fmt::Debug;

pub trait AddressableStorage: Debug {
    type T: Debug;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn get(&self, idx: usize) -> Option<Self::T>;

    unsafe fn get_unchecked(&self, idx: usize) -> Self::T;
}
