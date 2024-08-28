use crate::logical::{
    binder::{bind_context::BindContext, bind_query::bind_select::BoundSelect},
    logical_aggregate::LogicalAggregate,
    logical_filter::LogicalFilter,
    logical_limit::LogicalLimit,
    logical_order::LogicalOrder,
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
    pub fn plan(&self, mut select: BoundSelect) -> Result<LogicalOperator> {
        // Handle FROM
        let mut plan = FromPlanner::new(self.bind_context).plan(select.from)?;

        // Handle WHERE
        if let Some(mut filter) = select.filter {
            plan = SubqueryPlanner::new(self.bind_context).plan(&mut filter, plan)?;
            plan = LogicalOperator::Filter(LogicalNode {
                node: LogicalFilter { filter },
                location: LocationRequirement::Any,
                children: vec![plan],
                input_table_refs: None,
            });
        }

        // Handle GROUP BY/aggregates
        if !select.select_list.aggregates.is_empty() {
            let (mut group_exprs, grouping_sets) = match select.group_by {
                Some(group_by) => (group_by.expressions, group_by.grouping_sets),
                None => (Vec::new(), Vec::new()),
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

            plan = LogicalOperator::Aggregate(LogicalNode {
                node: agg,
                location: LocationRequirement::Any,
                children: vec![plan],
                input_table_refs: None, // TODO:
            })
        }

        // Handle HAVING
        if let Some(expr) = select.having {
            plan = LogicalOperator::Filter(LogicalNode {
                node: LogicalFilter { filter: expr },
                location: LocationRequirement::Any,
                children: vec![plan],
                input_table_refs: None,
            })
        }

        // Handle projections.
        for expr in &mut select.select_list.projections {
            plan = SubqueryPlanner::new(self.bind_context).plan(expr, plan)?;
        }
        plan = LogicalOperator::Project(LogicalNode {
            node: LogicalProject {
                projections: select.select_list.projections,
            },
            location: LocationRequirement::Any,
            children: vec![plan],
            input_table_refs: Some(vec![select.select_list.projections_table]),
        });

        // Handle ORDER BY
        if let Some(order_by) = select.order_by {
            plan = LogicalOperator::Order(LogicalNode {
                node: LogicalOrder {
                    exprs: order_by.exprs,
                },
                location: LocationRequirement::Any,
                children: vec![plan],
                input_table_refs: None, // TODO
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
                input_table_refs: None,
            });
        }

        // // Table to bring in scope to allow referencing the output of this
        // // select.
        // //
        // // Updated to pruned if necessary.
        // let mut final_table_ref = select.select_list.projections_table;

        // Omit any columns that shouldn't be in the output.
        if let Some(pruned) = select.select_list.pruned {
            plan = LogicalOperator::Project(LogicalNode {
                node: LogicalProject {
                    projections: pruned.expressions,
                },
                location: LocationRequirement::Any,
                children: vec![plan],
                input_table_refs: None, // TODO: ?
            })
        }

        Ok(plan)
    }
}
