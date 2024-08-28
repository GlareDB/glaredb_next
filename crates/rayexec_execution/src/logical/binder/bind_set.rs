use rayexec_bullet::datatype::DataType;
use rayexec_error::Result;
use rayexec_parser::ast;

use crate::{
    engine::vars::SessionVars,
    logical::{
        binder::expr_binder::{ExpressionBinder, RecursionContext},
        logical_set::{LogicalResetVar, LogicalSetVar, LogicalShowVar, VariableOrAll},
        operator::{LocationRequirement, LogicalNode},
        resolver::{resolve_context::ResolveContext, ResolvedMeta},
    },
};

use super::bind_context::{BindContext, BindScopeRef};

#[derive(Debug)]
pub struct SetVarBinder<'a> {
    pub current: BindScopeRef,
    pub vars: &'a SessionVars,
}

impl<'a> SetVarBinder<'a> {
    pub fn new(current: BindScopeRef, vars: &'a SessionVars) -> Self {
        SetVarBinder { current, vars }
    }

    pub fn bind_set(
        &self,
        bind_context: &mut BindContext,
        mut set: ast::SetVariable<ResolvedMeta>,
    ) -> Result<LogicalNode<LogicalSetVar>> {
        let expr = ExpressionBinder::new(self.current, &ResolveContext::empty()).bind_expression(
            bind_context,
            &set.value,
            RecursionContext {
                allow_window: false,
                allow_aggregate: false,
            },
        )?;

        let name = set.reference.pop()?; // TODO: Allow compound references?
        let value = expr.try_into_scalar()?;

        // Verify exists.
        let _ = self.vars.get_var(&name)?;

        Ok(LogicalNode {
            node: LogicalSetVar { name, value },
            location: LocationRequirement::ClientLocal,
            children: Vec::new(),
            input_table_refs: None,
        })
    }

    pub fn bind_reset(
        &self,
        _bind_context: &mut BindContext,
        reset: ast::ResetVariable<ResolvedMeta>,
    ) -> Result<LogicalNode<LogicalResetVar>> {
        let var = match reset.var {
            ast::VariableOrAll::Variable(mut v) => {
                let name = v.pop()?; // TODO: Allow compound references?
                let var = self.vars.get_var(&name)?;
                VariableOrAll::Variable(var.clone())
            }
            ast::VariableOrAll::All => VariableOrAll::All,
        };

        Ok(LogicalNode {
            node: LogicalResetVar { var },
            location: LocationRequirement::ClientLocal,
            children: Vec::new(),
            input_table_refs: None,
        })
    }

    pub fn bind_show(
        &self,
        bind_context: &mut BindContext,
        mut show: ast::ShowVariable<ResolvedMeta>,
    ) -> Result<LogicalNode<LogicalShowVar>> {
        let name = show.reference.pop()?; // TODO: Allow compound references?
        let var = self.vars.get_var(&name)?;

        bind_context.push_table(self.current, "", vec![DataType::Utf8], vec![name])?;

        Ok(LogicalNode {
            node: LogicalShowVar { var: var.clone() },
            location: LocationRequirement::ClientLocal, // Technically could be any since the variable is copied.
            children: Vec::new(),
            input_table_refs: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::logical::binder::bind_context::testutil::columns_in_scope;

    use super::*;

    #[test]
    fn bind_show_has_column() {
        let mut context = BindContext::new();
        let scope = context.root_scope_ref();

        let vars = SessionVars::new_local();
        let _ = SetVarBinder::new(scope, &vars)
            .bind_show(
                &mut context,
                ast::ShowVariable {
                    reference: vec!["application_name".to_string()].into(),
                },
            )
            .unwrap();

        let cols = columns_in_scope(&context, scope);
        let expected = vec![("application_name".to_string(), DataType::Utf8)];
        assert_eq!(expected, cols);
    }
}
