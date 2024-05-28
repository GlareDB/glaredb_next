use crate::{
    database::{catalog::CatalogTx, entry::TableEntry, table::DataTableInsert, DatabaseContext},
    planner::explainable::{ExplainConfig, ExplainEntry, Explainable},
};
use rayexec_bullet::batch::Batch;
use rayexec_error::Result;
use std::task::Context;

use super::{OperatorState, PartitionState, PhysicalOperator, PollPull, PollPush};

#[derive(Debug)]
pub struct InsertPartitionState {
    insert: Box<dyn DataTableInsert>,
}

#[derive(Debug)]
pub struct PhysicalInsert {
    catalog: String,
    schema: String,
    table: TableEntry,
}

impl PhysicalInsert {
    pub fn try_create_states(
        &self,
        context: &DatabaseContext,
        num_partitions: usize,
    ) -> Result<Vec<InsertPartitionState>> {
        // TODO: Placeholder.
        let tx = CatalogTx::new();

        let data_table = context
            .get_catalog(&self.catalog)?
            .get_schema(&tx, &self.schema)?
            .get_data_table(&tx, &self.table)?;

        // TODO: Pass constraints, on conflict
        let inserts = data_table.insert(num_partitions)?;

        let states = inserts
            .into_iter()
            .map(|insert| InsertPartitionState { insert })
            .collect();

        Ok(states)
    }
}

impl PhysicalOperator for PhysicalInsert {
    fn poll_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        batch: Batch,
    ) -> Result<PollPush> {
        match partition_state {
            PartitionState::Insert(state) => state.insert.poll_push(cx, batch),
            other => panic!("invalid partition state: {other:?}"),
        }
    }

    fn finalize_push(
        &self,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<()> {
        match partition_state {
            PartitionState::Insert(state) => state.insert.finalize(),
            other => panic!("invalid partition state: {other:?}"),
        }
    }

    fn poll_pull(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollPull> {
        unimplemented!("unsure")
    }
}

impl Explainable for PhysicalInsert {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Insert").with_value("table", &self.table.name)
    }
}
