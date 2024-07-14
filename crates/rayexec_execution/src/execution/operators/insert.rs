use crate::{
    database::{catalog::CatalogTx, entry::TableEntry, table::DataTableInsert, DatabaseContext},
    logical::explainable::{ExplainConfig, ExplainEntry, Explainable},
};
use rayexec_bullet::batch::Batch;
use rayexec_error::Result;
use std::task::{Context, Waker};

use super::{OperatorState, PartitionState, PhysicalOperator, PollFinalize, PollPull, PollPush};

#[derive(Debug)]
pub struct InsertPartitionState {
    insert: Box<dyn DataTableInsert>,
    finished: bool,

    // TODO: I'm not exactly sure where I want this. The idea is that inserts
    // are essentially async functions (e.g. `async fn insert_postgres(...)`)
    // and our contexts will be provided during the function call. But I don't
    // know if we want to rely solely on that.
    pull_waker: Option<Waker>,
}

#[derive(Debug)]
pub struct PhysicalInsert {
    catalog: String,
    schema: String,
    table: TableEntry,
}

impl PhysicalInsert {
    pub fn new(catalog: impl Into<String>, schema: impl Into<String>, table: TableEntry) -> Self {
        PhysicalInsert {
            catalog: catalog.into(),
            schema: schema.into(),
            table,
        }
    }

    pub fn try_create_states(
        &self,
        context: &DatabaseContext,
        num_partitions: usize,
    ) -> Result<Vec<InsertPartitionState>> {
        // TODO: Placeholder.
        let tx = CatalogTx::new();

        let data_table =
            context
                .get_catalog(&self.catalog)?
                .data_table(&tx, &self.schema, &self.table)?;

        // TODO: Pass constraints, on conflict
        let inserts = data_table.insert(num_partitions)?;

        let states = inserts
            .into_iter()
            .map(|insert| InsertPartitionState {
                insert,
                finished: false,
                pull_waker: None,
            })
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
            PartitionState::Insert(state) => {
                let poll = state.insert.poll_push(cx, batch)?;

                if let Some(waker) = state.pull_waker.take() {
                    waker.wake();
                }

                Ok(poll)
            }
            other => panic!("invalid partition state: {other:?}"),
        }
    }

    fn finalize_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        match partition_state {
            PartitionState::Insert(state) => {
                state.insert.finalize(cx)?;

                state.finished = true;
                if let Some(waker) = state.pull_waker.take() {
                    waker.wake();
                }

                Ok(PollFinalize::Finalized)
            }
            other => panic!("invalid partition state: {other:?}"),
        }
    }

    fn poll_pull(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollPull> {
        match partition_state {
            PartitionState::Insert(state) => {
                if state.finished {
                    Ok(PollPull::Exhausted)
                } else {
                    state.pull_waker = Some(cx.waker().clone());
                    Ok(PollPull::Pending)
                }
            }
            other => panic!("invalid partition state: {other:?}"),
        }
    }
}

impl Explainable for PhysicalInsert {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Insert").with_value("table", &self.table.name)
    }
}
