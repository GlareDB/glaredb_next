use crate::{
    expr::{cast_expr::CastExpr, column_expr::ColumnExpr, Expression},
    logical::{
        binder::{
            bind_context::{BindContext, BindScopeRef, Table, TableRef},
            bind_query::bind_setop::{BoundSetOp, SetOpCastRequirement},
        },
        logical_project::LogicalProject,
        logical_setop::LogicalSetop,
        operator::{LocationRequirement, LogicalOperator, Node},
        planner::plan_query::QueryPlanner,
    },
};
use rayexec_error::{RayexecError, Result};

pub struct SetOpPlanner<'a> {
    pub bind_context: &'a BindContext,
}

impl<'a> SetOpPlanner<'a> {
    pub fn new(bind_context: &'a BindContext) -> Self {
        SetOpPlanner { bind_context }
    }

    pub fn plan(&self, setop: BoundSetOp) -> Result<LogicalOperator> {
        let query_planner = QueryPlanner::new(self.bind_context);
        let mut left = query_planner.plan(*setop.left)?;
        let mut right = query_planner.plan(*setop.right)?;

        match setop.cast_req {
            SetOpCastRequirement::LeftNeedsCast(left_cast_ref) => {
                left = self.wrap_cast(left, setop.left_scope, left_cast_ref)?;
            }
            SetOpCastRequirement::RightNeedsCast(right_cast_ref) => {
                right = self.wrap_cast(right, setop.right_scope, right_cast_ref)?;
            }
            SetOpCastRequirement::BothNeedsCast {
                left_cast_ref,
                right_cast_ref,
            } => {
                left = self.wrap_cast(left, setop.left_scope, left_cast_ref)?;
                right = self.wrap_cast(right, setop.right_scope, right_cast_ref)?;
            }
            SetOpCastRequirement::None => (),
        }

        Ok(LogicalOperator::SetOp(Node {
            node: LogicalSetop {
                kind: setop.kind,
                all: setop.all,
                table_ref: setop.setop_table,
            },
            location: LocationRequirement::Any,
            children: vec![left, right],
        }))
    }

    fn wrap_cast(
        &self,
        orig_plan: LogicalOperator,
        orig_scope: BindScopeRef,
        cast_table_ref: TableRef,
    ) -> Result<LogicalOperator> {
        let orig_table = self.get_original_table(orig_scope)?;

        Ok(LogicalOperator::Project(Node {
            node: LogicalProject {
                projections: self.generate_cast_expressions(orig_table, cast_table_ref)?,
                projection_table: cast_table_ref,
            },
            location: LocationRequirement::Any,
            children: vec![orig_plan],
        }))
    }

    fn get_original_table(&self, scope_ref: BindScopeRef) -> Result<&Table> {
        let mut iter = self.bind_context.iter_tables(scope_ref)?;
        let table = match iter.next() {
            Some(table) => table,
            None => return Err(RayexecError::new("No table is scope")),
        };

        if iter.next().is_some() {
            // TODO: Is this possible?
            return Err(RayexecError::new("Too many tables in scope"));
        }

        Ok(table)
    }

    fn generate_cast_expressions(
        &self,
        orig_table: &Table,
        cast_table_ref: TableRef,
    ) -> Result<Vec<Expression>> {
        let cast_table = self.bind_context.get_table(cast_table_ref)?;

        let mut cast_exprs = Vec::with_capacity(orig_table.column_types.len());

        for (idx, (orig_type, need_type)) in orig_table
            .column_types
            .iter()
            .zip(&cast_table.column_types)
            .enumerate()
        {
            let col_expr = Expression::Column(ColumnExpr {
                table_scope: orig_table.reference,
                column: idx,
            });

            if orig_type == need_type {
                // No cast needed, reference original table.
                cast_exprs.push(col_expr);
                continue;
            }

            cast_exprs.push(Expression::Cast(CastExpr {
                to: need_type.clone(),
                expr: Box::new(col_expr),
            }));
        }

        Ok(cast_exprs)
    }
}
