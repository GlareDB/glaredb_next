use parking_lot::Mutex;
use rayexec_bullet::array::Array;
use rayexec_bullet::batch::Batch;
use rayexec_bullet::compute::filter::filter;
use rayexec_bullet::compute::take::take;
use rayexec_error::{RayexecError, Result};
use std::collections::VecDeque;
use std::task::Context;
use std::{sync::Arc, task::Waker};

use crate::execution::operators::{
    OperatorState, PartitionState, PhysicalOperator, PollPull, PollPush,
};
use crate::expr::PhysicalScalarExpression;
use crate::planner::operator::JoinType;

use super::join_hash_table::PartitionJoinHashTable;

#[derive(Debug)]
pub struct HashJoinBuildPartitionState {}

#[derive(Debug)]
pub struct HashJoinProbePartitionState {}

#[derive(Debug)]
pub struct HashJoinOperatorState {
    /// Shared output states containing possibly completed hash tables.
    output_states: Vec<Mutex<SharedOutputPartitionState>>,
}

#[derive(Debug)]
struct SharedOutputPartitionState {
    /// Completed hash tables from input partitions.
    completed: Vec<PartitionJoinHashTable>,

    /// Number of build input remaining for this partition.
    remaining: usize,

    /// If a thread tried to pull before this partition is ready to produce
    /// output.
    pull_waker: Option<Waker>,
}

#[derive(Debug)]
pub struct PhysicalHashJoin {
    /// The type of join we're performing (inner, left, right, semi, etc).
    join_type: JoinType,

    /// Column indices on the left (build) side we're joining on.
    left_on: Vec<usize>,

    /// Column indices on the right (probe) side we're joining on.
    right_on: Vec<usize>,
}

impl PhysicalHashJoin {
    pub fn new(join_type: JoinType, left_on: Vec<usize>, right_on: Vec<usize>) -> Self {
        PhysicalHashJoin {
            join_type,
            left_on,
            right_on,
        }
    }

    /// Create states for this operator.
    ///
    /// The number of partition inputs on the build side may be different than
    /// the number of partitions on the probe side.
    ///
    /// Output partitions equals the number of probe side input partitions.
    pub fn create_states(
        &self,
        build_partitions: usize,
        probe_partitions: usize,
    ) -> (
        HashJoinOperatorState,
        Vec<HashJoinBuildPartitionState>,
        Vec<HashJoinProbePartitionState>,
    ) {
        unimplemented!()
    }
}

impl PhysicalOperator for PhysicalHashJoin {
    fn poll_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
        batch: Batch,
    ) -> Result<PollPush> {
        unimplemented!()
    }

    fn finalize_push(
        &self,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<()> {
        unimplemented!()
    }

    fn poll_pull(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<PollPull> {
        unimplemented!()
    }
}
