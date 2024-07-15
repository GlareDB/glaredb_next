use crate::{
    functions::copy::{CopyToFunction, CopyToSink},
    logical::explainable::{ExplainConfig, ExplainEntry, Explainable},
};
use rayexec_bullet::batch::Batch;
use rayexec_error::{RayexecError, Result};
use rayexec_io::FileLocation;
use std::task::{Context, Waker};

use super::{OperatorState, PartitionState, PhysicalOperator, PollFinalize, PollPull, PollPush};

#[derive(Debug)]
pub enum CopyToPartitionState {
    Writing(Option<CopyToInnerPartitionState>),
    Finalizing(Option<CopyToInnerPartitionState>),
    Finished,
}

#[derive(Debug)]
pub struct CopyToInnerPartitionState {
    sink: Box<dyn CopyToSink>,
    pull_waker: Option<Waker>,
}

#[derive(Debug)]
pub struct PhysicalCopyTo {
    copy_to: Box<dyn CopyToFunction>,
    location: FileLocation,
}

impl PhysicalCopyTo {
    pub fn new(copy_to: Box<dyn CopyToFunction>, location: FileLocation) -> Self {
        PhysicalCopyTo { copy_to, location }
    }

    // TODO: Only allows a single input partition for now. Multiple partitions
    // would required writing to separate files. We'd want to append the
    // partition number to file location, but exact behavior is still tbd.
    pub fn try_create_states(&self, num_partitions: usize) -> Result<Vec<CopyToPartitionState>> {
        if num_partitions != 1 {
            return Err(RayexecError::new(
                "CopyTo operator only supports a single partition for now",
            ));
        }

        let states = self
            .copy_to
            .create_sinks(self.location.clone(), num_partitions)?
            .into_iter()
            .map(|sink| {
                CopyToPartitionState::Writing(Some(CopyToInnerPartitionState {
                    sink,
                    pull_waker: None,
                }))
            })
            .collect::<Vec<_>>();

        Ok(states)
    }
}

impl PhysicalOperator for PhysicalCopyTo {
    fn poll_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        batch: Batch,
    ) -> Result<PollPush> {
        match partition_state {
            PartitionState::CopyTo(state) => match state {
                CopyToPartitionState::Writing(Some(inner)) => {
                    let poll = match inner.sink.poll_push(cx, batch)? {
                        PollPush::Pending(batch) => return Ok(PollPush::Pending(batch)),
                        other => other,
                    };

                    if let Some(waker) = inner.pull_waker.take() {
                        waker.wake();
                    }

                    Ok(poll)
                }
                other => Err(RayexecError::new(format!(
                    "CopyTo operator in wrong state: {other:?}"
                ))),
            },
            other => panic!("invalid partition state: {other:?}"),
        }
    }

    fn poll_finalize_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        match partition_state {
            PartitionState::CopyTo(state) => match state {
                CopyToPartitionState::Writing(inner) => {
                    *state = CopyToPartitionState::Finalizing(inner.take());
                    self.poll_finalize_push(cx, partition_state, operator_state)
                }
                CopyToPartitionState::Finalizing(Some(inner)) => {
                    match inner.sink.poll_finalize(cx)? {
                        PollFinalize::Pending => return Ok(PollFinalize::Pending),
                        PollFinalize::Finalized => {
                            if let Some(waker) = inner.pull_waker.take() {
                                waker.wake();
                            }

                            *state = CopyToPartitionState::Finished;

                            Ok(PollFinalize::Finalized)
                        }
                    }
                }
                other => Err(RayexecError::new(format!(
                    "CopyTo operator in wrong state: {other:?}"
                ))),
            },
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
            PartitionState::CopyTo(state) => match state {
                CopyToPartitionState::Writing(inner) | CopyToPartitionState::Finalizing(inner) => {
                    inner
                        .as_mut()
                        .map(|inner| inner.pull_waker = Some(cx.waker().clone()));
                    Ok(PollPull::Pending)
                }
                CopyToPartitionState::Finished => Ok(PollPull::Exhausted),
            },
            other => panic!("invalid partition state: {other:?}"),
        }
    }
}

impl Explainable for PhysicalCopyTo {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("CopyTo")
    }
}
