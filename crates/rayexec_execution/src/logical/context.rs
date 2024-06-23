use super::operator::LogicalOperator;

#[derive(Debug, Clone, PartialEq)]
pub struct MaterializedPlan {
    /// Index within the query context.
    pub idx: usize,

    /// Number of operators that will be scanning the result of materialization.
    pub num_scans: usize,

    /// The root of the plan that will be materialized.
    pub root: LogicalOperator,
}

/// Additional query context to allow for more complex query graphs.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryContext {
    /// Plans that will be materialized during execution.
    ///
    /// This is used to allow for graph-like query plans to allow multiple
    /// operators be able read from the same plan.
    pub materialized: Vec<MaterializedPlan>,
}

impl QueryContext {
    /// Push a plan for materialization.
    ///
    /// The index of the plan within the query context will be returned.
    pub fn push_plan_for_materialization(
        &mut self,
        root: LogicalOperator,
        num_scans: usize,
    ) -> usize {
        let idx = self.materialized.len();
        self.materialized.push(MaterializedPlan {
            idx,
            num_scans,
            root,
        });
        idx
    }
}
