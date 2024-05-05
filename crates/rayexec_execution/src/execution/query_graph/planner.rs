use std::sync::Arc;

use crate::planner::operator::LogicalOperator;
use rayexec_error::Result;

use super::{sink::QuerySink, QueryGraph};

/// Configuration used for trigger debug condititions during planning.
#[derive(Debug, Clone, Copy, Default)]
pub struct QueryGraphDebugConfig {
    /// Trigger an error if we attempt to plan a nested loop join.
    pub error_on_nested_loop_join: bool,
}

/// Create a query graph from a logical plan.
#[derive(Debug)]
pub struct QueryGraphPlanner {
    /// Attempt to create pipelines with a target number of partitions.
    target_partitions: usize,

    debug_conf: QueryGraphDebugConfig,
}

impl QueryGraphPlanner {
    pub fn new(target_partitions: usize, debug_conf: QueryGraphDebugConfig) -> Self {
        QueryGraphPlanner {
            target_partitions,
            debug_conf,
        }
    }

    /// Create a query graph from a logical plan.
    ///
    /// The provided query sink will be where all the results of a query get
    /// pushed to (e.g. the client).
    pub fn create_graph(&self, plan: LogicalOperator, sink: QuerySink) -> Result<QueryGraph> {
        unimplemented!()
    }
}

#[derive(Debug)]
struct QueryGraphBuilder {
    target_partitions: usize,
    debug_conf: QueryGraphDebugConfig,
}
