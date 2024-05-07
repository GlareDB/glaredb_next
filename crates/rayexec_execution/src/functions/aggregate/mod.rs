pub trait AggregateState {
    /// Initialize this state.
    fn initialize(&mut self);

    fn combine(&mut self, other: Self);
}
