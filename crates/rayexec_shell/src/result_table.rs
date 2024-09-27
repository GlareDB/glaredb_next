use std::sync::Arc;
use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::{stream::Stream, StreamExt, TryStreamExt};
use rayexec_bullet::{
    batch::Batch, field::Schema, format::pretty::table::PrettyTable, row::ScalarRow,
};
use rayexec_error::Result;
use rayexec_execution::{
    engine::{profiler::PlanningProfileData, result::ExecutionResult},
    execution::executable::profiler::ExecutionProfileData,
    runtime::handle::QueryHandle,
};

#[derive(Debug)]
pub struct StreamingTable {
    pub(crate) result: ExecutionResult,
}

impl StreamingTable {
    pub fn schema(&self) -> &Schema {
        &self.result.output_schema
    }

    pub fn handle(&self) -> &Arc<dyn QueryHandle> {
        &self.result.handle
    }

    pub async fn collect(self) -> Result<MaterializedResultTable> {
        let batches: Vec<_> = self.result.stream.try_collect::<Vec<_>>().await?;

        Ok(MaterializedResultTable {
            schema: self.result.output_schema,
            batches,
            planning_profile: self.result.planning_profile,
            execution_profile: None,
        })
    }

    pub async fn collect_with_execution_profile(self) -> Result<MaterializedResultTable> {
        let batches: Vec<_> = self.result.stream.try_collect::<Vec<_>>().await?;
        let execution_profile = self.result.handle.generate_execution_profile_data().await?;

        Ok(MaterializedResultTable {
            schema: self.result.output_schema,
            batches,
            planning_profile: self.result.planning_profile,
            execution_profile: Some(execution_profile),
        })
    }

    pub async fn generate_profile_data(
        &self,
    ) -> Result<(PlanningProfileData, ExecutionProfileData)> {
        let execution_profile = self.result.handle.generate_execution_profile_data().await?;
        Ok((self.result.planning_profile.clone(), execution_profile))
    }
}

impl Stream for StreamingTable {
    type Item = Result<Batch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.result.stream.poll_next_unpin(cx)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MaterializedResultTable {
    pub(crate) schema: Schema,
    pub(crate) batches: Vec<Batch>,
    pub(crate) planning_profile: PlanningProfileData,
    pub(crate) execution_profile: Option<ExecutionProfileData>,
}

impl MaterializedResultTable {
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn planning_profile_data(&self) -> &PlanningProfileData {
        &self.planning_profile
    }

    pub fn execution_profile_data(&self) -> Option<&ExecutionProfileData> {
        self.execution_profile.as_ref()
    }

    pub fn pretty_table(&self, width: usize, max_rows: Option<usize>) -> Result<PrettyTable> {
        PrettyTable::try_new(&self.schema, &self.batches, width, max_rows)
    }

    pub fn iter_batches<'a>(&'a self) -> impl Iterator<Item = &'a Batch> {
        self.batches.iter()
    }

    pub fn iter_rows(&self) -> MaterializedRowIter {
        MaterializedRowIter {
            table: self,
            batch_idx: 0,
            row_idx: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MaterializedRowIter<'a> {
    table: &'a MaterializedResultTable,
    batch_idx: usize,
    row_idx: usize,
}

impl<'a> Iterator for MaterializedRowIter<'a> {
    type Item = ScalarRow<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let batch = self.table.batches.get(self.batch_idx)?;

            match batch.row(self.row_idx) {
                Some(row) => {
                    self.row_idx += 1;
                    return Some(row);
                }
                None => {
                    // Try next batch.
                    self.row_idx = 0;
                    self.batch_idx += 1;
                }
            }
        }
    }
}
