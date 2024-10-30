use std::fmt;

const HASH_PREFIX_MASK: u64 = 0xFFFFFFFF00000000;

pub const fn hash_prefix(hash: u64) -> u32 {
    ((hash & HASH_PREFIX_MASK) >> 32) as u32
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntryKey<T> {
    pub hash_prefix: u32,
    pub key: T,
}

impl<T> EntryKey<T>
where
    T: fmt::Debug + Clone + Copy + PartialEq + Eq,
{
    pub const fn new(hash: u64, key: T) -> Self {
        EntryKey {
            hash_prefix: hash_prefix(hash),
            key,
        }
    }

    pub const fn is_empty(&self) -> bool {
        match self.hash_prefix {
            0 => true,
            _ => false,
        }
    }

    pub const fn prefix_matches_hash(&self, hash: u64) -> bool {
        self.hash_prefix == hash_prefix(hash)
    }
}
