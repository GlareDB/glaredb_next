use crate::{
    functions::copy::{CopyToFunction, CopyToSink},
    logical::explainable::{ExplainConfig, ExplainEntry, Explainable},
};
use rayexec_bullet::batch::Batch;
use rayexec_error::Result;
use rayexec_io::FileLocation;
use std::task::Context;

use super::{OperatorState, PartitionState, PhysicalOperator, PollFinalize, PollPull, PollPush};

#[derive(Debug)]
pub enum CopyToPartitionState {
    Writing { sink: Box<dyn CopyToSink> },
}

#[derive(Debug)]
pub struct PhysicalCopyTo {
    copy_to: Box<dyn CopyToFunction>,
    location: FileLocation,
}

impl PhysicalCopyTo {}

impl PhysicalOperator for PhysicalCopyTo {
    fn poll_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        batch: Batch,
    ) -> Result<PollPush> {
        unimplemented!()
    }

    fn poll_finalize_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        unimplemented!()
    }

    fn poll_pull(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollPull> {
        unimplemented!()
    }
}

impl Explainable for PhysicalCopyTo {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("CopyTo")
    }
}
