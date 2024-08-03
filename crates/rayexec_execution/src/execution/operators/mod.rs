//! Implementations of physical operators in an execution pipeline.

pub mod copy_to;
pub mod create_schema;
pub mod create_table;
pub mod drop;
pub mod empty;
pub mod filter;
pub mod hash_aggregate;
pub mod insert;
pub mod ipc;
pub mod join;
pub mod limit;
pub mod materialize;
pub mod project;
pub mod round_robin;
pub mod scan;
pub mod simple;
pub mod sink;
pub mod sort;
pub mod table_function;
pub mod ungrouped_aggregate;
pub mod union;
pub mod values;

mod util;

#[cfg(test)]
mod test_util;

use copy_to::{CopyToPartitionState, PhysicalCopyTo};
use create_schema::{CreateSchemaPartitionState, PhysicalCreateSchema};
use create_table::{CreateTableOperatorState, CreateTablePartitionState, PhysicalCreateTable};
use drop::{DropPartitionState, PhysicalDrop};
use empty::PhysicalEmpty;
use filter::{FilterOperation, PhysicalFilter};
use hash_aggregate::PhysicalHashAggregate;
use insert::{InsertPartitionState, PhysicalInsert};
use join::hash_join::PhysicalHashJoin;
use join::nl_join::PhysicalNestedLoopJoin;
use limit::PhysicalLimit;
use materialize::{
    MaterializeOperatorState, MaterializePullPartitionState, MaterializePushPartitionState,
    PhysicalMaterialize,
};
use project::{PhysicalProject, ProjectOperation};
use rayexec_bullet::batch::Batch;
use rayexec_error::{OptionExt, Result};
use round_robin::PhysicalRoundRobinRepartition;
use scan::{PhysicalScan, ScanPartitionState};
use simple::SimpleOperator;
use sink::{PhysicalQuerySink, QuerySinkPartitionState};
use sort::local_sort::PhysicalLocalSort;
use sort::merge_sorted::PhysicalMergeSortedInputs;
use std::fmt::Debug;
use std::sync::Arc;
use std::task::Context;
use table_function::{PhysicalTableFunction, TableFunctionPartitionState};
use ungrouped_aggregate::{
    PhysicalUngroupedAggregate, UngroupedAggregateOperatorState, UngroupedAggregatePartitionState,
};
use union::{PhysicalUnion, UnionBottomPartitionState, UnionOperatorState, UnionTopPartitionState};
use values::PhysicalValues;

use crate::database::DatabaseContext;
use crate::logical::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::proto::DatabaseProtoConv;

use self::empty::EmptyPartitionState;
use self::hash_aggregate::{HashAggregateOperatorState, HashAggregatePartitionState};
use self::join::hash_join::{
    HashJoinBuildPartitionState, HashJoinOperatorState, HashJoinProbePartitionState,
};
use self::join::nl_join::{
    NestedLoopJoinBuildPartitionState, NestedLoopJoinOperatorState,
    NestedLoopJoinProbePartitionState,
};
use self::limit::LimitPartitionState;
use self::round_robin::{
    RoundRobinOperatorState, RoundRobinPullPartitionState, RoundRobinPushPartitionState,
};
use self::simple::SimplePartitionState;
use self::sort::local_sort::LocalSortPartitionState;
use self::sort::merge_sorted::{
    MergeSortedOperatorState, MergeSortedPullPartitionState, MergeSortedPushPartitionState,
};
use self::values::ValuesPartitionState;

/// States local to a partition within a single operator.
// Current size: 192 bytes
#[derive(Debug)]
pub enum PartitionState {
    HashAggregate(HashAggregatePartitionState),
    UngroupedAggregate(UngroupedAggregatePartitionState),
    NestedLoopJoinBuild(NestedLoopJoinBuildPartitionState),
    NestedLoopJoinProbe(NestedLoopJoinProbePartitionState),
    HashJoinBuild(HashJoinBuildPartitionState),
    HashJoinProbe(HashJoinProbePartitionState),
    Values(ValuesPartitionState),
    QuerySink(QuerySinkPartitionState),
    RoundRobinPush(RoundRobinPushPartitionState),
    RoundRobinPull(RoundRobinPullPartitionState),
    MergeSortedPush(MergeSortedPushPartitionState),
    MergeSortedPull(MergeSortedPullPartitionState),
    LocalSort(LocalSortPartitionState),
    Limit(LimitPartitionState),
    MaterializePush(MaterializePushPartitionState),
    MaterializePull(MaterializePullPartitionState),
    UnionTop(UnionTopPartitionState),
    UnionBottom(UnionBottomPartitionState),
    Simple(SimplePartitionState),
    Scan(ScanPartitionState),
    TableFunction(TableFunctionPartitionState),
    Insert(InsertPartitionState),
    CopyTo(CopyToPartitionState),
    CreateTable(CreateTablePartitionState),
    CreateSchema(CreateSchemaPartitionState),
    Drop(DropPartitionState),
    Empty(EmptyPartitionState),
    None,
}

/// A global state across all partitions in an operator.
// Current size: 112 bytes
#[derive(Debug)]
pub enum OperatorState {
    HashAggregate(HashAggregateOperatorState),
    UngroupedAggregate(UngroupedAggregateOperatorState),
    NestedLoopJoin(NestedLoopJoinOperatorState),
    HashJoin(HashJoinOperatorState),
    RoundRobin(RoundRobinOperatorState),
    MergeSorted(MergeSortedOperatorState),
    Materialize(MaterializeOperatorState),
    Union(UnionOperatorState),
    CreateTable(CreateTableOperatorState),
    None,
}

/// Result of a push to an operator.
///
/// An operator may not be ready to accept input either because it's waiting on
/// something else to complete (e.g. the right side of a join needs to the left
/// side to complete first) or some internal buffer is full.
#[derive(Debug, PartialEq)]
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

    /// Batch was successfully pushed, but the operator needs more input before
    /// it can start producing output
    NeedsMore,
}

/// Result of a pull from a Source.
#[derive(Debug, PartialEq)]
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

#[derive(Debug, PartialEq)]
pub enum PollFinalize {
    Finalized,
    Pending,
}

/// Describes the relationships of partition states for operators.
#[derive(Debug)]
pub enum InputOutputStates {
    /// Input and output partition states have a one-to-one mapping.
    ///
    /// The states used for pushing to an operator are the same states used to
    /// pull from the operator.
    ///
    /// This variant should also be used for pure source and pure sink operators
    /// where states are only ever used for pushing or pulling.
    OneToOne {
        /// Per-partition operators states.
        ///
        /// Length of vec determines the partitioning (parallelism) of the
        /// operator.
        partition_states: Vec<PartitionState>,
    },

    /// Operators accepts multiple inputs, and a single output.
    ///
    /// A single set of input states are used during pull.
    NaryInputSingleOutput {
        /// Per-input, per-partition operators states.
        ///
        /// The outer vec matches the number of inputs to an operator (e.g. a
        /// join should have two).
        partition_states: Vec<Vec<PartitionState>>,

        /// Index into the above vec to determine which set of states are used
        /// for pulling.
        ///
        /// For joins, the partition states for probes are the ones used for
        /// pulling.
        ///
        /// The chosen set of states indicates the output partitioning for the
        /// operator.
        pull_states: usize,
    },

    /// Partition states for an operator that accepts a single input, and
    /// produce 'n' outputs.
    SingleInputNaryOutput {
        /// States for the single input during push.
        push_states: Vec<PartitionState>,

        /// States for the n outputs.
        pull_states: Vec<Vec<PartitionState>>,
    },

    /// Partition states between the push side and pull side are separate.
    ///
    /// This provides a way for operators to output a different number of
    /// partitions than it receives.
    ///
    /// Operators that need this will introduce a pipeline split where the push
    /// states are used for pipeline's sink, while the pull states are used for
    /// the source of a separate pipeline.
    SeparateInputOutput {
        /// States used during push.
        push_states: Vec<PartitionState>,

        /// States used during pull.
        pull_states: Vec<PartitionState>,
    },
}

/// States generates from an operator to use during execution.
#[derive(Debug)]
pub struct ExecutionStates {
    /// Global operator state.
    pub operator_state: Arc<OperatorState>,

    /// Partition states for the operator.
    pub partition_states: InputOutputStates,
}

pub trait ExecutableOperator: Sync + Send + Debug + Explainable {
    /// Create execution states for this operator.
    ///
    /// `input_partitions` is the partitioning for each input that will be
    /// pushing batches through this operator.
    ///
    /// Joins are assumed to have two inputs.
    fn create_states(
        &self,
        _context: &DatabaseContext,
        _partitions: Vec<usize>,
    ) -> Result<ExecutionStates>;

    /// Try to push a batch for this partition.
    fn poll_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
        batch: Batch,
    ) -> Result<PollPush>;

    /// Finalize pushing to partition.
    ///
    /// This indicates the operator will receive no more input for a given
    /// partition, allowing the operator to execution some finalization logic.
    fn poll_finalize_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<PollFinalize>;

    /// Try to pull a batch for this partition.
    fn poll_pull(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<PollPull>;
}

// 128 bytes
#[derive(Debug)]
pub enum PhysicalOperator {
    HashAggregate(PhysicalHashAggregate),
    UngroupedAggregate(PhysicalUngroupedAggregate),
    NestedLoopJoin(PhysicalNestedLoopJoin),
    HashJoin(PhysicalHashJoin),
    Values(PhysicalValues),
    QuerySink(PhysicalQuerySink),
    RoundRobin(PhysicalRoundRobinRepartition),
    MergeSorted(PhysicalMergeSortedInputs),
    LocalSort(PhysicalLocalSort),
    Limit(PhysicalLimit),
    Materialize(PhysicalMaterialize),
    Union(PhysicalUnion),
    Filter(SimpleOperator<FilterOperation>),
    Project(SimpleOperator<ProjectOperation>),
    Scan(PhysicalScan),
    TableFunction(PhysicalTableFunction),
    Insert(PhysicalInsert),
    CopyTo(PhysicalCopyTo),
    CreateTable(PhysicalCreateTable),
    CreateSchema(PhysicalCreateSchema),
    Drop(PhysicalDrop),
    Empty(PhysicalEmpty),
}

impl ExecutableOperator for PhysicalOperator {
    fn create_states(
        &self,
        context: &DatabaseContext,
        partitions: Vec<usize>,
    ) -> Result<ExecutionStates> {
        match self {
            Self::HashAggregate(op) => op.create_states(context, partitions),
            Self::UngroupedAggregate(op) => op.create_states(context, partitions),
            Self::NestedLoopJoin(op) => op.create_states(context, partitions),
            Self::HashJoin(op) => op.create_states(context, partitions),
            Self::Values(op) => op.create_states(context, partitions),
            Self::QuerySink(op) => op.create_states(context, partitions),
            Self::RoundRobin(op) => op.create_states(context, partitions),
            Self::MergeSorted(op) => op.create_states(context, partitions),
            Self::LocalSort(op) => op.create_states(context, partitions),
            Self::Limit(op) => op.create_states(context, partitions),
            Self::Materialize(op) => op.create_states(context, partitions),
            Self::Union(op) => op.create_states(context, partitions),
            Self::Filter(op) => op.create_states(context, partitions),
            Self::Project(op) => op.create_states(context, partitions),
            Self::Scan(op) => op.create_states(context, partitions),
            Self::TableFunction(op) => op.create_states(context, partitions),
            Self::Insert(op) => op.create_states(context, partitions),
            Self::CopyTo(op) => op.create_states(context, partitions),
            Self::CreateTable(op) => op.create_states(context, partitions),
            Self::CreateSchema(op) => op.create_states(context, partitions),
            Self::Drop(op) => op.create_states(context, partitions),
            Self::Empty(op) => op.create_states(context, partitions),
        }
    }

    fn poll_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
        batch: Batch,
    ) -> Result<PollPush> {
        match self {
            Self::HashAggregate(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::UngroupedAggregate(op) => {
                op.poll_push(cx, partition_state, operator_state, batch)
            }
            Self::NestedLoopJoin(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::HashJoin(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::Values(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::QuerySink(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::RoundRobin(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::MergeSorted(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::LocalSort(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::Limit(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::Materialize(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::Union(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::Filter(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::Project(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::Scan(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::TableFunction(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::Insert(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::CopyTo(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::CreateTable(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::CreateSchema(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::Drop(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::Empty(op) => op.poll_push(cx, partition_state, operator_state, batch),
        }
    }

    fn poll_finalize_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        match self {
            Self::HashAggregate(op) => op.poll_finalize_push(cx, partition_state, operator_state),
            Self::UngroupedAggregate(op) => {
                op.poll_finalize_push(cx, partition_state, operator_state)
            }
            Self::NestedLoopJoin(op) => op.poll_finalize_push(cx, partition_state, operator_state),
            Self::HashJoin(op) => op.poll_finalize_push(cx, partition_state, operator_state),
            Self::Values(op) => op.poll_finalize_push(cx, partition_state, operator_state),
            Self::QuerySink(op) => op.poll_finalize_push(cx, partition_state, operator_state),
            Self::RoundRobin(op) => op.poll_finalize_push(cx, partition_state, operator_state),
            Self::MergeSorted(op) => op.poll_finalize_push(cx, partition_state, operator_state),
            Self::LocalSort(op) => op.poll_finalize_push(cx, partition_state, operator_state),
            Self::Limit(op) => op.poll_finalize_push(cx, partition_state, operator_state),
            Self::Materialize(op) => op.poll_finalize_push(cx, partition_state, operator_state),
            Self::Union(op) => op.poll_finalize_push(cx, partition_state, operator_state),
            Self::Filter(op) => op.poll_finalize_push(cx, partition_state, operator_state),
            Self::Project(op) => op.poll_finalize_push(cx, partition_state, operator_state),
            Self::Scan(op) => op.poll_finalize_push(cx, partition_state, operator_state),
            Self::TableFunction(op) => op.poll_finalize_push(cx, partition_state, operator_state),
            Self::Insert(op) => op.poll_finalize_push(cx, partition_state, operator_state),
            Self::CopyTo(op) => op.poll_finalize_push(cx, partition_state, operator_state),
            Self::CreateTable(op) => op.poll_finalize_push(cx, partition_state, operator_state),
            Self::CreateSchema(op) => op.poll_finalize_push(cx, partition_state, operator_state),
            Self::Drop(op) => op.poll_finalize_push(cx, partition_state, operator_state),
            Self::Empty(op) => op.poll_finalize_push(cx, partition_state, operator_state),
        }
    }

    fn poll_pull(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<PollPull> {
        match self {
            Self::HashAggregate(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::UngroupedAggregate(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::NestedLoopJoin(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::HashJoin(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::Values(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::QuerySink(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::RoundRobin(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::MergeSorted(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::LocalSort(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::Limit(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::Materialize(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::Union(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::Filter(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::Project(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::Scan(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::TableFunction(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::Insert(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::CopyTo(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::CreateTable(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::CreateSchema(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::Drop(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::Empty(op) => op.poll_pull(cx, partition_state, operator_state),
        }
    }
}

impl Explainable for PhysicalOperator {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        match self {
            Self::HashAggregate(op) => op.explain_entry(conf),
            Self::UngroupedAggregate(op) => op.explain_entry(conf),
            Self::NestedLoopJoin(op) => op.explain_entry(conf),
            Self::HashJoin(op) => op.explain_entry(conf),
            Self::Values(op) => op.explain_entry(conf),
            Self::QuerySink(op) => op.explain_entry(conf),
            Self::RoundRobin(op) => op.explain_entry(conf),
            Self::MergeSorted(op) => op.explain_entry(conf),
            Self::LocalSort(op) => op.explain_entry(conf),
            Self::Limit(op) => op.explain_entry(conf),
            Self::Materialize(op) => op.explain_entry(conf),
            Self::Union(op) => op.explain_entry(conf),
            Self::Filter(op) => op.explain_entry(conf),
            Self::Project(op) => op.explain_entry(conf),
            Self::Scan(op) => op.explain_entry(conf),
            Self::TableFunction(op) => op.explain_entry(conf),
            Self::Insert(op) => op.explain_entry(conf),
            Self::CopyTo(op) => op.explain_entry(conf),
            Self::CreateTable(op) => op.explain_entry(conf),
            Self::CreateSchema(op) => op.explain_entry(conf),
            Self::Drop(op) => op.explain_entry(conf),
            Self::Empty(op) => op.explain_entry(conf),
        }
    }
}

impl DatabaseProtoConv for PhysicalOperator {
    type ProtoType = rayexec_proto::generated::execution::PhysicalOperator;

    fn to_proto_ctx(&self, context: &DatabaseContext) -> Result<Self::ProtoType> {
        use rayexec_proto::generated::execution::physical_operator::Value;

        let value = match self {
            Self::CreateSchema(op) => Value::CreateSchema(op.to_proto_ctx(context)?),
            Self::CreateTable(op) => Value::CreateTable(op.to_proto_ctx(context)?),
            Self::Drop(op) => Value::Drop(op.to_proto_ctx(context)?),
            Self::Empty(op) => Value::Empty(op.to_proto_ctx(context)?),
            Self::Filter(op) => Value::Filter(op.to_proto_ctx(context)?),
            Self::Project(op) => Value::Project(op.to_proto_ctx(context)?),
            Self::Insert(op) => Value::Insert(op.to_proto_ctx(context)?),
            Self::Limit(op) => Value::Limit(op.to_proto_ctx(context)?),
            Self::Materialize(op) => Value::Materialize(op.to_proto_ctx(context)?),
            Self::Scan(op) => Value::Scan(op.to_proto_ctx(context)?),
            Self::UngroupedAggregate(op) => Value::UngroupedAggregate(op.to_proto_ctx(context)?),
            Self::Union(op) => Value::Union(op.to_proto_ctx(context)?),
            Self::Values(op) => Value::Values(op.to_proto_ctx(context)?),
            _ => unimplemented!(),
        };

        Ok(Self::ProtoType { value: Some(value) })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        use rayexec_proto::generated::execution::physical_operator::Value;

        Ok(match proto.value.required("value")? {
            Value::CreateSchema(op) => {
                PhysicalOperator::CreateSchema(PhysicalCreateSchema::from_proto_ctx(op, context)?)
            }
            Value::CreateTable(op) => {
                PhysicalOperator::CreateTable(PhysicalCreateTable::from_proto_ctx(op, context)?)
            }
            Value::Drop(op) => PhysicalOperator::Drop(PhysicalDrop::from_proto_ctx(op, context)?),
            Value::Empty(op) => {
                PhysicalOperator::Empty(PhysicalEmpty::from_proto_ctx(op, context)?)
            }
            Value::Filter(op) => {
                PhysicalOperator::Filter(PhysicalFilter::from_proto_ctx(op, context)?)
            }
            Value::Project(op) => {
                PhysicalOperator::Project(PhysicalProject::from_proto_ctx(op, context)?)
            }
            Value::Insert(op) => {
                PhysicalOperator::Insert(PhysicalInsert::from_proto_ctx(op, context)?)
            }
            Value::Limit(op) => {
                PhysicalOperator::Limit(PhysicalLimit::from_proto_ctx(op, context)?)
            }
            Value::Materialize(op) => {
                PhysicalOperator::Materialize(PhysicalMaterialize::from_proto_ctx(op, context)?)
            }
            Value::Scan(op) => PhysicalOperator::Scan(PhysicalScan::from_proto_ctx(op, context)?),
            Value::UngroupedAggregate(op) => PhysicalOperator::UngroupedAggregate(
                PhysicalUngroupedAggregate::from_proto_ctx(op, context)?,
            ),
            Value::Union(op) => {
                PhysicalOperator::Union(PhysicalUnion::from_proto_ctx(op, context)?)
            }
            Value::Values(op) => {
                PhysicalOperator::Values(PhysicalValues::from_proto_ctx(op, context)?)
            }

            _ => unimplemented!(),
        })
    }
}
