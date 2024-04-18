/// Backing storage for primitive values.
///
/// Currently this contains only a single variant, but should be extension point
/// for working with externally managed data (Arrow arrays from arrow-rs, shared
/// memory regions, CUDA, etc).
#[derive(Debug, PartialEq)]
pub enum PrimitiveStorage<T> {
    Vec(Vec<T>),
}

impl<T> PrimitiveStorage<T> {
    pub fn get(&self, idx: usize) -> Option<&T> {
        match self {
            Self::Vec(v) => v.get(idx),
        }
    }
}
