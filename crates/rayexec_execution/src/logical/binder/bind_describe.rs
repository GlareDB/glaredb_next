use rayexec_bullet::{
    datatype::DataType,
    field::{Field, Schema},
};
use rayexec_error::Result;
use rayexec_parser::ast;

use crate::logical::{
    binder::bind_query::{bind_from::FromBinder, QueryBinder},
    logical_describe::LogicalDescribe,
    operator::{LocationRequirement, LogicalNode},
    resolver::{resolve_context::ResolveContext, ResolvedMeta},
};

use super::bind_context::{BindContext, BindScopeRef};

#[derive(Debug)]
pub struct DescribeBinder<'a> {
    pub current: BindScopeRef,
    pub resolve_context: &'a ResolveContext,
}

impl<'a> DescribeBinder<'a> {
    pub fn new(current: BindScopeRef, resolve_context: &'a ResolveContext) -> Self {
        DescribeBinder {
            current,
            resolve_context,
        }
    }

    pub fn bind_describe(
        &self,
        bind_context: &mut BindContext,
        describe: ast::Describe<ResolvedMeta>,
    ) -> Result<LogicalNode<LogicalDescribe>> {
        bind_context.push_table(
            self.current,
            None,
            vec![DataType::Utf8, DataType::Utf8],
            vec!["column_name".to_string(), "datatype".to_string()],
        )?;

        let query_scope = bind_context.new_orphan_scope();

        // We don't care about the results of the bind, just the changes it
        // makes to the bind context (columns).
        match describe {
            ast::Describe::Query(query) => {
                let _ = QueryBinder::new(query_scope, self.resolve_context)
                    .bind(bind_context, query)?;
            }
            ast::Describe::FromNode(from) => {
                let _ = FromBinder::new(query_scope, self.resolve_context)
                    .bind(bind_context, Some(from))?;
            }
        }

        let fields = bind_context.iter_tables(query_scope)?.flat_map(|t| {
            t.column_names
                .iter()
                .zip(&t.column_types)
                .map(|(name, datatype)| Field::new(name, datatype.clone(), true))
        });

        Ok(LogicalNode {
            node: LogicalDescribe {
                schema: Schema::new(fields),
            },
            location: LocationRequirement::Any,
            children: Vec::new(),
            input_table_refs: None,
        })
    }
}
