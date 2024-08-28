use std::collections::HashMap;

use rayexec_error::{RayexecError, Result};
use rayexec_parser::ast;

use crate::logical::{
    logical_attach::{LogicalAttachDatabase, LogicalDetachDatabase},
    operator::{LocationRequirement, LogicalNode},
    resolver::{resolve_context::ResolveContext, ResolvedMeta},
};

use super::{
    bind_context::{BindContext, BindScopeRef},
    expr_binder::{ExpressionBinder, RecursionContext},
};

#[derive(Debug)]
pub enum BoundAttach {
    Database(LogicalNode<LogicalAttachDatabase>),
}

#[derive(Debug)]
pub enum BoundDetach {
    Database(LogicalNode<LogicalDetachDatabase>),
}

#[derive(Debug)]
pub struct AttachBinder {
    pub current: BindScopeRef,
}

impl AttachBinder {
    pub fn new(current: BindScopeRef) -> Self {
        AttachBinder { current }
    }

    pub fn bind_attach(
        &self,
        bind_context: &mut BindContext,
        mut attach: ast::Attach<ResolvedMeta>,
    ) -> Result<BoundAttach> {
        match attach.attach_type {
            ast::AttachType::Database => {
                let mut options = HashMap::new();

                for (k, v) in attach.options {
                    let k = k.into_normalized_string();
                    let expr = ExpressionBinder::new(self.current, &ResolveContext::empty())
                        .bind_expression(
                            bind_context,
                            &v,
                            RecursionContext {
                                allow_window: false,
                                allow_aggregate: false,
                            },
                        )?;
                    let v = expr.try_into_scalar()?;

                    if options.contains_key(&k) {
                        return Err(RayexecError::new(format!(
                            "Option '{k}' provided more than once"
                        )));
                    }
                    options.insert(k, v);
                }

                if attach.alias.0.len() != 1 {
                    return Err(RayexecError::new(format!(
                        "Expected a single identifier, got '{}'",
                        attach.alias
                    )));
                }
                let name = attach.alias.pop()?;
                let datasource = attach.datasource_name;

                // Currently this always has a "client local" requirement. This
                // essentially means catalog management happens on the client
                // (currently). For hybrid exec, the client-local catalog acts
                // as a stub.
                //
                // The semantics for this may change when we have a real
                // cloud-based catalog. Even then, this logic can still make
                // sense where the client calls a remote endpoint for persisting
                // catalog changes.
                Ok(BoundAttach::Database(LogicalNode {
                    node: LogicalAttachDatabase {
                        datasource: datasource.into_normalized_string(),
                        name,
                        options,
                    },
                    location: LocationRequirement::ClientLocal,
                    children: Vec::new(),
                    input_table_refs: None,
                }))
            }
            ast::AttachType::Table => Err(RayexecError::new("Attach tables not yet supported")),
        }
    }

    pub fn bind_detach(
        &self,
        _bind_context: &mut BindContext,
        mut detach: ast::Detach<ResolvedMeta>,
    ) -> Result<BoundDetach> {
        match detach.attach_type {
            ast::AttachType::Database => {
                if detach.alias.0.len() != 1 {
                    return Err(RayexecError::new(format!(
                        "Expected a single identifier, got '{}'",
                        detach.alias
                    )));
                }
                let name = detach.alias.pop()?;

                Ok(BoundDetach::Database(LogicalNode {
                    node: LogicalDetachDatabase { name },
                    location: LocationRequirement::ClientLocal,
                    children: Vec::new(),
                    input_table_refs: None,
                }))
            }
            ast::AttachType::Table => Err(RayexecError::new("Detach tables not yet supported")),
        }
    }
}