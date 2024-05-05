pub mod filter;
pub mod project;
pub mod query_sink;
pub mod simple;

use rayexec_bullet::batch::Batch;
use rayexec_error::Result;
use std::fmt::Debug;
use std::task::Context;

use self::query_sink::QuerySinkPartitionState;
use self::simple::SimplePartitionState;

/// States local to a partition within a single operator.
#[derive(Debug)]
pub enum PartitionState {
    QuerySink(QuerySinkPartitionState),
    Simple(SimplePartitionState),
    None,
}

/// A global state across all partitions in an operator.
#[derive(Debug)]
pub enum OperatorState {
    Simple(()),
    None,
}

/// Result of a push to an operator.
///
/// An operator may not be ready to accept input either because it's waiting on
/// something else to complete (e.g. the right side of a join needs to the left
/// side to complete first) or some internal buffer is full.
#[derive(Debug)]
pub enum PollPush {
    /// Batch was successfully pushed.
    Pushed,

    /// Batch could not be processed right now.
    ///
    /// A waker will be registered for a later wakeup. This same batch should be
    /// pushed at that time.
    Pending(Batch),

    /// This operator requires no more input.
    ///
    /// `finalize_push` for the operator should _not_ be called.
    Break,
}

/// Result of a pull from a Source.
#[derive(Debug)]
pub enum PollPull {
    /// Successfully received a data batch.
    Batch(Batch),

    /// A batch could not be be retrieved right now.
    ///
    /// A waker will be registered for a later wakeup to try to pull the next
    /// batch.
    Pending,

    /// The operator has been exhausted for this partition.
    Exhausted,
}

pub trait PhysicalOperator: Sync + Send + Debug {
    /// Try to push a batch for this partition.
    fn poll_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
        batch: Batch,
        input: usize,
        partition: usize,
    ) -> Result<PollPush>;

    /// Finalize pushing to partition.
    ///
    /// This indicates the operator will receive no more input for a given
    /// partition, allowing the operator to execution some finalization logic.
    fn finalize_push(
        &self,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
        input: usize,
        partition: usize,
    ) -> Result<()>;

    /// Try to pull a batch for this partition.
    fn poll_pull(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
        partition: usize,
    ) -> Result<PollPull>;
}
