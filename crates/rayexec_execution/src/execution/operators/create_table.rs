use crate::{
    database::{
        catalog::CatalogTx,
        create::CreateTableInfo,
        table::{DataTable, DataTableInsert},
        DatabaseContext,
    },
    logical::explainable::{ExplainConfig, ExplainEntry, Explainable},
};
use futures::{future::BoxFuture, FutureExt};
use parking_lot::Mutex;
use rayexec_bullet::batch::Batch;
use rayexec_error::Result;
use std::task::{Context, Poll};
use std::{fmt, task::Waker};

use super::{OperatorState, PartitionState, PhysicalOperator, PollPull, PollPush};

pub enum CreateTablePartitionState {
    /// State when we're creating the table.
    Creating {
        /// Future for creating the table.
        create: BoxFuture<'static, Result<Box<dyn DataTable>>>,

        /// After creation, how many insert partitions we'll want to make.
        insert_partitions: usize,

        /// Index of this partition.
        partition_idx: usize,

        pull_waker: Option<Waker>,
    },

    /// State when we're inserting into the new table.
    Inserting {
        /// Insert into the new table.
        ///
        /// If None, global state should be checked.
        insert: Option<Box<dyn DataTableInsert>>,

        /// Index of this partition.
        partition_idx: usize,

        /// If we're done inserting.
        finished: bool,

        pull_waker: Option<Waker>,
    },
}

impl fmt::Debug for CreateTablePartitionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CreateTablePartitionState").finish()
    }
}

#[derive(Debug)]
pub struct CreateTableOperatorState {
    shared: Mutex<SharedState>,
}

#[derive(Debug)]
struct SharedState {
    inserts: Vec<Option<Box<dyn DataTableInsert>>>,
    push_wakers: Vec<Option<Waker>>,
}

#[derive(Debug)]
pub struct PhysicalCreateTable {
    catalog: String,
    schema: String,
    info: CreateTableInfo,
}

impl PhysicalCreateTable {
    pub fn new(
        catalog: impl Into<String>,
        schema: impl Into<String>,
        info: CreateTableInfo,
    ) -> Self {
        PhysicalCreateTable {
            catalog: catalog.into(),
            schema: schema.into(),
            info,
        }
    }

    pub fn try_create_states(
        &self,
        context: &DatabaseContext,
        insert_partitions: usize,
    ) -> Result<(CreateTableOperatorState, Vec<CreateTablePartitionState>)> {
        // TODO: Placeholder.
        let tx = CatalogTx::new();

        let catalog = context.get_catalog(&self.catalog)?.catalog_modifier(&tx)?;
        let create = catalog.create_table(&self.schema, self.info.clone());

        // First partition will be responsible for the create.
        let mut states = vec![CreateTablePartitionState::Creating {
            create,
            insert_partitions,
            partition_idx: 0,
            pull_waker: None,
        }];

        // Rest of the partitions will start on insert, waiting until the first
        // partition completes.
        states.extend(
            (1..insert_partitions).map(|idx| CreateTablePartitionState::Inserting {
                insert: None,
                partition_idx: idx,
                finished: false,
                pull_waker: None,
            }),
        );

        let operator_state = CreateTableOperatorState {
            shared: Mutex::new(SharedState {
                inserts: (0..insert_partitions).map(|_| None).collect(),
                push_wakers: vec![None; insert_partitions],
            }),
        };

        Ok((operator_state, states))
    }
}

impl PhysicalOperator for PhysicalCreateTable {
    fn poll_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
        batch: Batch,
    ) -> Result<PollPush> {
        match partition_state {
            PartitionState::CreateTable(CreateTablePartitionState::Creating {
                create,
                insert_partitions,
                partition_idx,
                pull_waker,
            }) => match create.poll_unpin(cx) {
                Poll::Ready(Ok(table)) => {
                    let inserts = table.insert(*insert_partitions)?;
                    let mut inserts: Vec<_> = inserts.into_iter().map(Some).collect();

                    let mut shared = match operator_state {
                        OperatorState::CreateTable(state) => state.shared.lock(),
                        other => panic!("invalid operator state: {other:?}"),
                    };

                    let insert = inserts[*partition_idx].take();
                    shared.inserts = inserts;

                    for waker in shared.push_wakers.iter_mut() {
                        if let Some(waker) = waker.take() {
                            waker.wake();
                        }
                    }

                    *partition_state =
                        PartitionState::CreateTable(CreateTablePartitionState::Inserting {
                            insert,
                            partition_idx: *partition_idx,
                            finished: false,
                            pull_waker: pull_waker.take(),
                        });
                    // Continue on, we'll be doing the insert in the below match.
                }
                Poll::Ready(Err(e)) => return Err(e),
                Poll::Pending => return Ok(PollPush::Pending(batch)),
            },
            PartitionState::CreateTable(_) => (), // Fall through to below match.
            other => panic!("invalid partition state: {other:?}"),
        }

        match partition_state {
            PartitionState::CreateTable(CreateTablePartitionState::Inserting {
                insert,
                partition_idx,
                ..
            }) => {
                if insert.is_none() {
                    let mut shared = match operator_state {
                        OperatorState::CreateTable(state) => state.shared.lock(),
                        other => panic!("invalid operator state: {other:?}"),
                    };

                    if shared.inserts[*partition_idx].is_none() {
                        shared.push_wakers[*partition_idx] = Some(cx.waker().clone());
                        return Ok(PollPush::Pending(batch));
                    }

                    *insert = shared.inserts[*partition_idx].take();
                }

                let insert = insert.as_mut().expect("insert to be Some");
                // Insert will store the context if it returns pending.
                insert.poll_push(cx, batch)
            }
            other => panic!("invalid partition state: {other:?}"),
        }
    }

    fn finalize_push(
        &self,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<()> {
        match partition_state {
            PartitionState::CreateTable(CreateTablePartitionState::Inserting {
                finished,
                pull_waker,
                ..
            }) => {
                *finished = true;
                if let Some(waker) = pull_waker.take() {
                    waker.wake();
                }
                Ok(())
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
            PartitionState::CreateTable(CreateTablePartitionState::Inserting {
                finished,
                pull_waker,
                ..
            }) => {
                if *finished {
                    return Ok(PollPull::Exhausted);
                }
                *pull_waker = Some(cx.waker().clone());
                Ok(PollPull::Pending)
            }
            PartitionState::CreateTable(CreateTablePartitionState::Creating {
                pull_waker, ..
            }) => {
                *pull_waker = Some(cx.waker().clone());
                Ok(PollPull::Pending)
            }
            other => panic!("invalid partition state: {other:?}"),
        }
    }
}

impl Explainable for PhysicalCreateTable {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("CreateTable").with_value("table", &self.info.name)
    }
}
