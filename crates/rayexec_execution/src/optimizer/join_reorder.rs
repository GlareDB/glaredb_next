use std::collections::VecDeque;

use crate::{
    expr::Expression,
    logical::{
        binder::bind_context::BindContext,
        logical_join::JoinType,
        operator::{LogicalNode, LogicalOperator},
    },
};
use rayexec_error::Result;

use super::{
    filter_pushdown::{extracted_filter::ExtractedFilter, split::split_conjunction},
    OptimizeRule,
};

/// Reorders joins in the plan.
///
/// Currently just does some reordering or filters + cross joins, but will
/// support switching join sides based on statistics eventually.
#[derive(Debug, Default)]
pub struct JoinReorder {}

impl OptimizeRule for JoinReorder {
    fn optimize(
        &mut self,
        bind_context: &mut BindContext,
        plan: LogicalOperator,
    ) -> Result<LogicalOperator> {
        unimplemented!()
    }
}

#[derive(Debug, Default)]
struct FilterAndPlanExtractor {
    filters: Vec<ExtractedFilter>,
    child_plans: Vec<LogicalOperator>,
}

impl FilterAndPlanExtractor {
    // TODO: Duplicated with filter pushdown.
    fn add_filter(&mut self, expr: Expression) {
        let mut split = Vec::new();
        split_conjunction(expr, &mut split);

        self.filters
            .extend(split.into_iter().map(ExtractedFilter::from_expr))
    }

    fn reorder(&mut self, mut root: LogicalOperator) -> Result<LogicalOperator> {
        match &root {
            LogicalOperator::Filter(_) | LogicalOperator::CrossJoin(_) => {
                self.extract_filters_and_join_children(root)?;
            }
            LogicalOperator::ComparisonJoin(join) if join.node.join_type == JoinType::Inner => {
                self.extract_filters_and_join_children(root)?;
            }
            _ => {
                // Can't extract at this node, try reordering children and
                // return.
                root.modify_replace_children(&mut |child| {
                    let mut extractor = Self::default();
                    extractor.reorder(child)
                })?;
                return Ok(root);
            }
        }

        let candidates: Vec<_> = self
            .child_plans
            .drain(..)
            .map(|c| {
                let refs = c.get_output_table_refs();
                (c, refs)
            })
            .collect();

        unimplemented!()
    }

    fn extract_filters_and_join_children(&mut self, root: LogicalOperator) -> Result<()> {
        assert!(self.filters.is_empty());
        assert!(self.child_plans.is_empty());

        let mut queue: VecDeque<_> = [root].into_iter().collect();

        while let Some(plan) = queue.pop_front() {
            match plan {
                LogicalOperator::Filter(filter) => {
                    self.add_filter(filter.node.filter);
                }
                LogicalOperator::CrossJoin(mut join) => {
                    for child in join.children.drain(..) {
                        queue.push_back(child);
                    }
                }
                LogicalOperator::ComparisonJoin(mut join) => {
                    if join.node.join_type == JoinType::Inner {
                        for condition in join.node.conditions {
                            self.add_filter(condition.into_expression());
                        }
                        for child in join.children.drain(..) {
                            queue.push_back(child);
                        }
                    } else {
                        // Nothing we can do (yet).
                        self.child_plans.push(LogicalOperator::ComparisonJoin(join))
                    }
                }
                other => self.child_plans.push(other),
            }
        }

        unimplemented!()
    }
}
