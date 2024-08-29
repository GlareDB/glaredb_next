use crate::logical::{
    binder::{bind_context::BindContext, bind_query::bind_select::BoundSelect},
    logical_aggregate::LogicalAggregate,
    logical_filter::LogicalFilter,
    logical_limit::LogicalLimit,
    logical_order::LogicalOrder,
    logical_project::LogicalProject,
    operator::{LocationRequirement, LogicalOperator, Node},
    planner::{plan_from::FromPlanner, plan_subquery::SubqueryPlanner},
};
use rayexec_error::Result;

#[derive(Debug)]
pub struct SelectPlanner<'a> {
    pub bind_context: &'a BindContext,
}

impl<'a> SelectPlanner<'a> {
    pub fn plan(&self, mut select: BoundSelect) -> Result<LogicalOperator> {
        // Handle FROM
        let mut plan = FromPlanner::new(self.bind_context).plan(select.from)?;

        // Handle WHERE
        if let Some(mut filter) = select.filter {
            plan = SubqueryPlanner::new(self.bind_context).plan(&mut filter, plan)?;
            plan = LogicalOperator::Filter(Node {
                node: LogicalFilter { filter },
                location: LocationRequirement::Any,
                children: vec![plan],
            });
        }

        // Handle GROUP BY/aggregates
        if !select.select_list.aggregates.is_empty() {
            let (mut group_exprs, grouping_sets) = match select.group_by {
                Some(group_by) => (group_by.expressions, Some(group_by.grouping_sets)),
                None => (Vec::new(), None),
            };

            for expr in &mut group_exprs {
                plan = SubqueryPlanner::new(self.bind_context).plan(expr, plan)?;
            }

            for expr in &mut select.select_list.aggregates {
                plan = SubqueryPlanner::new(self.bind_context).plan(expr, plan)?;
            }

            let agg = LogicalAggregate {
                aggregates: select.select_list.aggregates,
                group_exprs,
                grouping_sets,
            };

            plan = LogicalOperator::Aggregate(Node {
                node: agg,
                location: LocationRequirement::Any,
                children: vec![plan],
            })
        }

        // Handle HAVING
        if let Some(expr) = select.having {
            plan = LogicalOperator::Filter(Node {
                node: LogicalFilter { filter: expr },
                location: LocationRequirement::Any,
                children: vec![plan],
            })
        }

        // Handle projections.
        for expr in &mut select.select_list.projections {
            plan = SubqueryPlanner::new(self.bind_context).plan(expr, plan)?;
        }
        plan = LogicalOperator::Project(Node {
            node: LogicalProject {
                projections: select.select_list.projections,
                projection_table: select.select_list.projections_table,
            },
            location: LocationRequirement::Any,
            children: vec![plan],
        });

        // Handle ORDER BY
        if let Some(order_by) = select.order_by {
            plan = LogicalOperator::Order(Node {
                node: LogicalOrder {
                    exprs: order_by.exprs,
                },
                location: LocationRequirement::Any,
                children: vec![plan],
            })
        }

        // Handle LIMIT
        if let Some(limit) = select.limit {
            plan = LogicalOperator::Limit(Node {
                node: LogicalLimit {
                    offset: limit.offset,
                    limit: limit.limit,
                },
                location: LocationRequirement::Any,
                children: vec![plan],
            });
        }

        // // Table to bring in scope to allow referencing the output of this
        // // select.
        // //
        // // Updated to pruned if necessary.
        // let mut final_table_ref = select.select_list.projections_table;

        // Omit any columns that shouldn't be in the output.
        if let Some(pruned) = select.select_list.pruned {
            plan = LogicalOperator::Project(Node {
                node: LogicalProject {
                    projections: pruned.expressions,
                    projection_table: pruned.table,
                },
                location: LocationRequirement::Any,
                children: vec![plan],
            })
        }

        Ok(plan)
    }
}
