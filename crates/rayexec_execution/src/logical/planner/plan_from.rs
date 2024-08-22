use rayexec_error::Result;
use rayexec_parser::ast;

use crate::logical::resolver::{resolve_context::ResolveContext, ResolvedMeta};

use super::{plan_statement::LogicalQuery, planning_context::PlanningContext};

#[derive(Debug)]
pub struct FromPlanner<'a> {
    pub resolve_context: &'a ResolveContext,
}

impl<'a> FromPlanner<'a> {
    pub fn plan_from(
        &self,
        context: &mut PlanningContext,
        from: ast::FromNode<ResolvedMeta>,
    ) -> Result<LogicalQuery> {
        unimplemented!()
    }
}
