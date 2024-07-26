use std::collections::HashMap;

use crate::execution::pipeline::PipelineId;

use super::IntermediatePipeline;

#[derive(Debug)]
pub struct IntermediatePipelinePlanner {}

impl IntermediatePipelinePlanner {}

#[derive(Debug)]
struct BuildConfig {}

/// Used for ensuring every pipeline in a query has a unique id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PipelineIdGen {
    gen: PipelineId,
}

impl PipelineIdGen {
    fn next(&mut self) -> PipelineId {
        let id = self.gen;
        self.gen.0 += 1;
        id
    }
}

#[derive(Debug, Default)]
struct Materializations {
    /// Source pipelines for `MaterializeScan` operators.
    ///
    /// Key corresponds to the index of the materialized plan in the
    /// QueryContext. Since multiple pipelines can read from the same
    /// materialization, each key has a vec of pipelines that we take from.
    materialize_sources: HashMap<usize, Vec<IntermediatePipeline>>,
}

impl Materializations {
    /// Checks if there's any pipelines still in the map.
    ///
    /// This is used as a debugging check. After planning the entire query, all
    /// pending pipelines should have been consumed. If there's still pipelines,
    /// that means we're not accuratately tracking the number of materialized
    /// scans.
    fn has_remaining_pipelines(&self) -> bool {
        for pipelines in self.materialize_sources.values() {
            if !pipelines.is_empty() {
                return true;
            }
        }
        false
    }
}

#[derive(Debug)]
struct IntermediatePipelineBuildState {
    in_progress: Option<IntermediatePipeline>,

    completed: Vec<IntermediatePipeline>,
}
