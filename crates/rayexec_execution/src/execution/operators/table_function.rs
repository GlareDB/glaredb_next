use crate::{
    database::table::DataTableScan,
    functions::table::PlannedTableFunction,
    logical::explainable::{ExplainConfig, ExplainEntry, Explainable},
    runtime::ExecutionRuntime,
};
use futures::{future::BoxFuture, FutureExt};
use rayexec_bullet::batch::Batch;
use rayexec_error::{RayexecError, Result};
use std::sync::Arc;
use std::task::Context;
use std::{fmt, task::Poll};

use super::{
    util::futures::make_static, OperatorState, PartitionState, PhysicalOperator, PollFinalize,
    PollPull, PollPush,
};

pub struct TableFunctionPartitionState {
    scan: Box<dyn DataTableScan>,
    /// In progress pull we're working on.
    future: Option<BoxFuture<'static, Result<Option<Batch>>>>,
}

impl fmt::Debug for TableFunctionPartitionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableFunctionPartitionState")
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct PhysicalTableFunction {
    function: Box<dyn PlannedTableFunction>,
}

impl PhysicalTableFunction {
    pub fn new(function: Box<dyn PlannedTableFunction>) -> Self {
        PhysicalTableFunction { function }
    }

    pub fn try_create_states(
        &self,
        num_partitions: usize,
    ) -> Result<Vec<TableFunctionPartitionState>> {
        let data_table = self.function.datatable()?;

        // TODO: Pushdown projections, filters
        let scans = data_table.scan(num_partitions)?;

        let states = scans
            .into_iter()
            .map(|scan| TableFunctionPartitionState { scan, future: None })
            .collect();

        Ok(states)
    }
}

impl PhysicalOperator for PhysicalTableFunction {
    fn poll_push(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        _batch: Batch,
    ) -> Result<PollPush> {
        // Could UNNEST be implemented as a table function?
        Err(RayexecError::new("Cannot push to physical table function"))
    }

    fn poll_finalize_push(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        Err(RayexecError::new("Cannot push to physical table function"))
    }

    fn poll_pull(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollPull> {
        match partition_state {
            PartitionState::TableFunction(state) => {
                if let Some(future) = &mut state.future {
                    match future.poll_unpin(cx) {
                        Poll::Ready(Ok(Some(batch))) => {
                            state.future = None; // Future complete, next pull with create a new one.
                            return Ok(PollPull::Batch(batch));
                        }
                        Poll::Ready(Ok(None)) => return Ok(PollPull::Exhausted),
                        Poll::Ready(Err(e)) => return Err(e),
                        Poll::Pending => return Ok(PollPull::Pending),
                    }
                }

                let mut future = state.scan.pull();
                match future.poll_unpin(cx) {
                    Poll::Ready(Ok(Some(batch))) => Ok(PollPull::Batch(batch)),
                    Poll::Ready(Ok(None)) => Ok(PollPull::Exhausted),
                    Poll::Ready(Err(e)) => Err(e),
                    Poll::Pending => {
                        // SAFETY: Scan lives on the partition state and
                        // outlives this future.
                        state.future = Some(unsafe { make_static(future) });
                        Ok(PollPull::Pending)
                    }
                }
            }
            other => panic!("invalid partition state: {other:?}"),
        }
    }
}

impl Explainable for PhysicalTableFunction {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("TableFunction")
    }
}
