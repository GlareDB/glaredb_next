use crate::logical::{
    binder::{bind_context::BindContext, bound_select::BoundSelect},
    logical_aggregate::LogicalAggregate,
    logical_filter::LogicalFilter,
    logical_limit::LogicalLimit,
    logical_order::{LogicalOrder, OrderByExpr},
    logical_project::LogicalProject,
    operator::{LocationRequirement, LogicalNode, LogicalOperator},
    planner::{plan_from::FromPlanner, plan_subquery::SubqueryPlanner},
};
use rayexec_error::Result;

#[derive(Debug)]
pub struct SelectPlanner<'a> {
    pub bind_context: &'a BindContext,
}

impl<'a> SelectPlanner<'a> {
    pub fn new(bind_context: &'a BindContext) -> Self {
        SelectPlanner { bind_context }
    }

    pub fn plan(&self, mut select: BoundSelect) -> Result<LogicalOperator> {
        // Handle FROM
        let mut plan = FromPlanner::new(self.bind_context).plan(select.from)?;

        // Handle WHERE
        if let Some(mut filter) = select.filter {
            plan = SubqueryPlanner::new(self.bind_context).plan(&mut filter, plan)?;

            // Do it
            unimplemented!()
        }

        // Handle GROUP BY/aggregates
        if !select.aggregates.is_empty() {
            let (mut group_exprs, grouping_sets) = match select.group_by {
                Some(group_by) => (group_by.expressions, group_by.grouping_sets),
                None => (Vec::new(), Vec::new()),
            };

            for expr in &mut group_exprs {
                plan = SubqueryPlanner::new(self.bind_context).plan(expr, plan)?;
            }

            for expr in &mut select.aggregates {
                plan = SubqueryPlanner::new(self.bind_context).plan(expr, plan)?;
            }

            let agg = LogicalAggregate {
                aggregates: (0..select.aggregates.len()).collect(),
                group_exprs: (0..group_exprs.len())
                    .map(|i| i + select.aggregates.len())
                    .collect(),
                grouping_sets,
            };

            let mut expressions = select.aggregates;
            expressions.append(&mut group_exprs);

            plan = LogicalOperator::Aggregate(LogicalNode {
                node: agg,
                location: LocationRequirement::Any,
                children: vec![plan],
                expressions,
            })
        }

        // Handle HAVING
        if let Some(expr) = select.having {
            plan = LogicalOperator::Filter(LogicalNode {
                node: LogicalFilter,
                location: LocationRequirement::Any,
                children: vec![plan],
                expressions: vec![expr],
            })
        }

        // Handle projections.
        let projection_len = select.projections.len(); // Used to see if need a separate projection at the end.
        for expr in &mut select.projections {
            plan = SubqueryPlanner::new(self.bind_context).plan(expr, plan)?;
        }
        plan = LogicalOperator::Project(LogicalNode {
            node: LogicalProject,
            location: LocationRequirement::Any,
            children: vec![plan],
            expressions: select.projections,
        });

        // Handle ORDER BY
        if let Some(order_by) = select.order_by {
            let mut exprs = Vec::with_capacity(order_by.exprs.len());
            let mut order_by_exprs = Vec::with_capacity(order_by.exprs.len());

            for (idx, expr) in order_by.exprs.into_iter().enumerate() {
                order_by_exprs.push(OrderByExpr {
                    expr: idx,
                    desc: expr.desc,
                    nulls_first: expr.nulls_first,
                });
                exprs.push(expr.expr);
            }

            plan = LogicalOperator::Order(LogicalNode {
                node: LogicalOrder {
                    exprs: order_by_exprs,
                },
                location: LocationRequirement::Any,
                children: vec![plan],
                expressions: exprs,
            })
        }

        // Handle LIMIT
        if let Some(limit) = select.limit {
            plan = LogicalOperator::Limit(LogicalNode {
                node: LogicalLimit {
                    offset: limit.offset,
                    limit: limit.limit,
                },
                location: LocationRequirement::Any,
                children: vec![plan],
                expressions: Vec::new(),
            });
        }

        // Omit any columns that shouldn't be in the output.
        if projection_len > select.output_columns {
            // Do the thing...
            unimplemented!()
        }

        Ok(plan)
    }
}
