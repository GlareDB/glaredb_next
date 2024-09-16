pub mod condition_extractor;
pub mod split;

use std::collections::HashSet;

use rayexec_error::Result;
use split::split_conjunction;

use crate::{
    expr::Expression,
    logical::{
        binder::bind_context::{BindContext, TableRef},
        logical_filter::LogicalFilter,
        logical_join::LogicalCrossJoin,
        operator::{LocationRequirement, LogicalOperator, Node},
    },
};

use super::OptimizeRule;

/// Holds a filtering expression and all table refs the expression references.
#[derive(Debug)]
struct ExtractedFilter {
    /// The filter expression.
    filter: Expression,
    /// Tables refs this expression references.
    tables_refs: HashSet<TableRef>,
}

impl ExtractedFilter {
    fn from_expr(expr: Expression) -> Self {
        fn inner(child: &Expression, refs: &mut HashSet<TableRef>) {
            match child {
                Expression::Column(col) => {
                    refs.insert(col.table_scope);
                }
                other => other
                    .for_each_child(&mut |child| {
                        inner(child, refs);
                        Ok(())
                    })
                    .expect("getting table refs to not fail"),
            }
        }

        let mut refs = HashSet::new();
        inner(&expr, &mut refs);

        ExtractedFilter {
            filter: expr,
            tables_refs: refs,
        }
    }
}

#[derive(Debug, Default)]
pub struct FilterPushdownRule {
    filters: Vec<ExtractedFilter>,
}

impl OptimizeRule for FilterPushdownRule {
    fn optimize(
        &mut self,
        bind_context: &mut BindContext,
        plan: LogicalOperator,
    ) -> Result<LogicalOperator> {
        match plan {
            LogicalOperator::Filter(filter) => self.pushdown_filter(bind_context, filter),
            // LogicalOperator::CrossJoin(join) => self.pushdown_cross_join(bind_context, join),
            other => self.stop_pushdown(bind_context, other),
        }
    }
}

impl FilterPushdownRule {
    /// Stops the push down for this set of filters, and wraps the plan in a new
    /// filter node.
    ///
    /// This will go ahead and perform a separate pushdown to children of this
    /// plan.
    fn stop_pushdown(
        &mut self,
        bind_context: &mut BindContext,
        mut plan: LogicalOperator,
    ) -> Result<LogicalOperator> {
        // Continue with a separate pushdown step for the children.
        let mut children = Vec::with_capacity(plan.children().len());
        for mut child in plan.children_mut().drain(..) {
            let mut pushdown = FilterPushdownRule::default();
            child = pushdown.optimize(bind_context, child)?;
            children.push(child)
        }
        *plan.children_mut() = children;

        if self.filters.is_empty() {
            // No remaining filters.
            return Ok(plan);
        }

        let filter = Expression::and_all(self.filters.drain(..).map(|ex| ex.filter))
            .expect("expression to be created from non-empty iter");

        Ok(LogicalOperator::Filter(Node {
            node: LogicalFilter { filter },
            location: LocationRequirement::Any,
            children: vec![plan],
        }))
    }

    /// Pushes down through a filter node.
    ///
    /// This will extract the filter expressions from this node and append them
    /// to the rule's current filter list.
    fn pushdown_filter(
        &mut self,
        bind_context: &mut BindContext,
        mut plan: Node<LogicalFilter>,
    ) -> Result<LogicalOperator> {
        let child = plan.take_one_child_exact()?;

        let mut split = Vec::new();
        split_conjunction(plan.node.filter, &mut split);

        self.filters
            .extend(split.into_iter().map(ExtractedFilter::from_expr));

        self.optimize(bind_context, child)
    }

    fn pushdown_cross_join(
        &mut self,
        bind_context: &mut BindContext,
        plan: Node<LogicalCrossJoin>,
    ) -> Result<LogicalOperator> {
        unimplemented!()
    }
}
