pub mod planner;
pub mod sink;

use super::pipeline::Pipeline;

#[derive(Debug)]
pub struct QueryGraph {
    /// All pipelines that make up this query.
    pipelines: Vec<Pipeline>,
}
