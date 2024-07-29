use crate::database::DatabaseContext;
use crate::execution::query_graph::sink::PartitionSink;
use crate::logical::explainable::{ExplainConfig, ExplainEntry, Explainable};
use rayexec_bullet::batch::Batch;
use rayexec_error::{RayexecError, Result};
use std::task::Context;

use super::{
    ExecutionStates, OperatorState, PartitionState, PhysicalOperator, PollFinalize, PollPull,
    PollPush,
};

#[derive(Debug)]
pub struct QuerySinkPartitionStateBak {
    sink: Box<dyn PartitionSink>,
}

impl QuerySinkPartitionStateBak {
    pub fn new(sink: Box<dyn PartitionSink>) -> Self {
        QuerySinkPartitionStateBak { sink }
    }
}

/// Wrapper around a query sink to implement the physical operator trait.
#[derive(Debug)]
pub struct PhysicalQuerySink;

impl PhysicalOperator for PhysicalQuerySink {
    fn create_states(
        &self,
        context: &DatabaseContext,
        partitions: Vec<usize>,
    ) -> Result<ExecutionStates> {
        unimplemented!()
    }

    fn poll_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        batch: Batch,
    ) -> Result<PollPush> {
        let state = match partition_state {
            PartitionState::QuerySinkBak(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        state.sink.poll_push(cx, batch)
    }

    fn poll_finalize_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        let state = match partition_state {
            PartitionState::QuerySinkBak(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        state.sink.poll_finalize_push(cx)
    }

    fn poll_pull(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollPull> {
        Err(RayexecError::new("Query sink cannot be pulled from"))
    }
}

impl Explainable for PhysicalQuerySink {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("QuerySink")
    }
}
