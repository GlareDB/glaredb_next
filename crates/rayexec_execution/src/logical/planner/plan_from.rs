use rayexec_error::{not_implemented, RayexecError, Result};

use crate::{
    expr::{column_expr::ColumnExpr, Expression},
    logical::{
        binder::{
            bind_context::BindContext,
            bind_query::bind_from::{BoundFrom, BoundFromItem, BoundJoin},
        },
        logical_empty::LogicalEmpty,
        logical_join::{ComparisonCondition, JoinType, LogicalCrossJoin},
        logical_project::LogicalProject,
        logical_scan::{LogicalScan, ScanSource},
        operator::{LocationRequirement, LogicalNode, LogicalOperator, Node},
    },
};

use super::plan_query::QueryPlanner;

#[derive(Debug)]
pub struct FromPlanner<'a> {
    pub bind_context: &'a BindContext,
}

impl<'a> FromPlanner<'a> {
    pub fn new(bind_context: &'a BindContext) -> Self {
        FromPlanner { bind_context }
    }

    pub fn plan(&self, from: BoundFrom) -> Result<LogicalOperator> {
        match from.item {
            BoundFromItem::BaseTable(table) => {
                let mut types = Vec::new();
                let mut names = Vec::new();
                for table in self.bind_context.iter_tables(from.bind_ref)? {
                    types.extend(table.column_types.iter().cloned());
                    names.extend(table.column_names.iter().cloned());
                }

                let projection = (0..types.len()).collect();

                Ok(LogicalOperator::Scan(Node {
                    node: LogicalScan {
                        table_ref: table.table_ref,
                        types,
                        names,
                        projection,
                        source: ScanSource::Table {
                            catalog: table.catalog,
                            schema: table.schema,
                            source: table.entry,
                        },
                    },
                    location: table.location,
                    children: Vec::new(),
                }))
            }
            BoundFromItem::Join(_) => unimplemented!(),
            BoundFromItem::TableFunction(func) => {
                let mut types = Vec::new();
                let mut names = Vec::new();
                for table in self.bind_context.iter_tables(from.bind_ref)? {
                    types.extend(table.column_types.iter().cloned());
                    names.extend(table.column_names.iter().cloned());
                }

                let projection = (0..types.len()).collect();

                Ok(LogicalOperator::Scan(Node {
                    node: LogicalScan {
                        table_ref: func.table_ref,
                        types,
                        names,
                        projection,
                        source: ScanSource::TableFunction {
                            function: func.function,
                        },
                    },
                    location: func.location,
                    children: Vec::new(),
                }))
            }
            BoundFromItem::Subquery(subquery) => {
                let planner = QueryPlanner::new(self.bind_context);
                let plan = planner.plan(*subquery.subquery)?;

                // Project subquery columns into this scope.
                //
                // The binding scope for a subquery is nested relative to a
                // parent scope, so this project lets us resolve all columns
                // without special-casing from binding.
                let mut projections = Vec::new();
                for table_ref in plan.get_output_table_refs() {
                    let table = self.bind_context.get_table(table_ref)?;
                    for col_idx in 0..table.num_columns() {
                        projections.push(Expression::Column(ColumnExpr {
                            table_scope: table_ref,
                            column: col_idx,
                        }));
                    }
                }

                Ok(LogicalOperator::Project(Node {
                    node: LogicalProject {
                        projections,
                        projection_table: subquery.table_ref,
                    },
                    location: LocationRequirement::Any,
                    children: vec![plan],
                }))
            }
            BoundFromItem::Empty => Ok(LogicalOperator::Empty(Node {
                node: LogicalEmpty,
                location: LocationRequirement::Any,
                children: Vec::new(),
            })),
        }
    }

    fn plan_join(&self, join: BoundJoin) -> Result<LogicalOperator> {
        if join.lateral {
            not_implemented!("LATERAL join")
        }

        let left = self.plan(*join.left)?;
        let right = self.plan(*join.right)?;

        // Cross join.
        if join.conditions.is_empty() {
            if !join.conditions.is_empty() {
                return Err(RayexecError::new("CROSS JOIN should not have conditions"));
            }
            return Ok(LogicalOperator::CrossJoin(Node {
                node: LogicalCrossJoin,
                location: LocationRequirement::Any,
                children: vec![left, right],
            }));
        }

        unimplemented!()
    }
}
