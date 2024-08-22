use rayexec_error::Result;
use rayexec_parser::ast;

use crate::logical::resolver::{resolve_context::BindContext, Bound};

use super::{plan_statement::LogicalQuery, planning_context::PlanningContext};

#[derive(Debug)]
pub struct FromPlanner<'a> {
    pub bind_data: &'a BindContext,
}

impl<'a> FromPlanner<'a> {
    pub fn plan_from(
        &self,
        context: &mut PlanningContext,
        from: ast::FromNode<Bound>,
    ) -> Result<LogicalQuery> {
        unimplemented!()
    }
}
