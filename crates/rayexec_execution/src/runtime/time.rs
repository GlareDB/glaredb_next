/// Instant provides an abstraction around getting the current time, and
/// computing a duration from two instances.
///
/// This trait is needed to allow for runtime-specific implementations since
/// WASM does not support fetching the current time using the std function.
pub trait RuntimeInstant {
    /// Gets an instant representing now.
    fn now() -> Self;

    /// Returns the elapsed duration between `earlier` and `self`.
    ///
    /// `earlier` is later than `self`, this should return a duration
    /// representing zero.
    fn duration_since(&self, earlier: Self) -> std::time::Duration;
}
