use crate::{
    database::table::DataTableScan,
    engine::EngineRuntime,
    functions::table::{GenericTableFunction, TableFunctionArgs},
    logical::explainable::{ExplainConfig, ExplainEntry, Explainable},
};
use rayexec_bullet::batch::Batch;
use rayexec_error::{RayexecError, Result};
use std::sync::Arc;
use std::task::Context;

use super::{OperatorState, PartitionState, PhysicalOperator, PollPull, PollPush};

#[derive(Debug)]
pub struct TableFunctionPartitionState {
    scan: Box<dyn DataTableScan>,
}

#[derive(Debug)]
pub struct PhysicalTableFunction {
    function: Box<dyn GenericTableFunction>,
    args: TableFunctionArgs,
}

impl PhysicalTableFunction {
    pub fn new(function: Box<dyn GenericTableFunction>, args: TableFunctionArgs) -> Self {
        PhysicalTableFunction { function, args }
    }

    pub fn try_create_states(
        &self,
        runtime: &Arc<EngineRuntime>,
        num_partitions: usize, // yes
    ) -> Result<Vec<TableFunctionPartitionState>> {
        let mut specialized = self.function.specialize(&self.args)?;
        let data_table = specialized.datatable(runtime)?;

        // TODO: Pushdown projections, filters
        let scans = data_table.scan(num_partitions)?;

        let states = scans
            .into_iter()
            .map(|scan| TableFunctionPartitionState { scan })
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

    fn finalize_push(
        &self,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<()> {
        Err(RayexecError::new("Cannot push to physical table function"))
    }

    fn poll_pull(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollPull> {
        match partition_state {
            PartitionState::TableFunction(state) => state.scan.poll_pull(cx),
            other => panic!("invalid partition state: {other:?}"),
        }
    }
}

impl Explainable for PhysicalTableFunction {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("TableFunction")
    }
}
