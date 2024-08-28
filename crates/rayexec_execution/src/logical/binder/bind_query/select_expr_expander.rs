use crate::logical::{
    binder::bind_context::{BindContext, BindScopeRef, TableAlias},
    resolver::{resolve_context::ResolveContext, ResolvedMeta},
};
use rayexec_error::{RayexecError, Result};
use rayexec_parser::ast;

/// Expands wildcards in expressions found in the select list.
///
/// Generates ast expressions.
#[derive(Debug)]
pub struct SelectExprExpander<'a> {
    pub current: BindScopeRef,
    pub bind_context: &'a BindContext,
}

impl<'a> SelectExprExpander<'a> {
    pub fn new(current: BindScopeRef, bind_context: &'a BindContext) -> Self {
        SelectExprExpander {
            current,
            bind_context,
        }
    }

    pub fn expand_all_select_exprs(
        &self,
        exprs: impl IntoIterator<Item = ast::SelectExpr<ResolvedMeta>>,
    ) -> Result<Vec<ast::SelectExpr<ResolvedMeta>>> {
        let mut expanded = Vec::new();
        for expr in exprs {
            let mut ex = self.expand_select_expr(expr)?;
            expanded.append(&mut ex);
        }
        Ok(expanded)
    }

    pub fn expand_select_expr(
        &self,
        expr: ast::SelectExpr<ResolvedMeta>,
    ) -> Result<Vec<ast::SelectExpr<ResolvedMeta>>> {
        Ok(match expr {
            ast::SelectExpr::Wildcard(_wildcard) => {
                // TODO: Exclude, replace
                let mut exprs = Vec::new();
                for table in self.bind_context.iter_tables(self.current)? {
                    let alias_idents = match &table.alias {
                        Some(alias) => vec![ast::Ident::from_string(&alias.table)], // TODO: Schema + database too
                        None => Vec::new(),
                    };

                    for column in &table.column_names {
                        let mut idents = alias_idents.clone();
                        idents.push(ast::Ident::from_string(column));

                        exprs.push(ast::SelectExpr::Expr(ast::Expr::CompoundIdent(idents)))
                    }
                }

                exprs
            }
            ast::SelectExpr::QualifiedWildcard(reference, _wildcard) => {
                // TODO: Exclude, replace
                if reference.0.len() > 1 {
                    return Err(RayexecError::new(
                        "Qualified wildcard references with more than one ident not yet supported",
                    ));
                }

                // TODO: Get schema + catalog too if they exist.
                let table = reference.base()?.into_normalized_string();
                let alias = TableAlias {
                    database: None,
                    schema: None,
                    table,
                };

                let table = self
                    .bind_context
                    .iter_tables(self.current)?
                    .find(|t| match &t.alias {
                        Some(have_alias) => have_alias.matches(&alias),
                        None => false,
                    })
                    .ok_or_else(|| {
                        RayexecError::new(format!(
                            "Missing table '{alias}', cannot expand wildcard"
                        ))
                    })?;

                let mut exprs = Vec::new();
                for column in &table.column_names {
                    // TODO: Include shcema + database too.
                    exprs.push(ast::SelectExpr::Expr(ast::Expr::CompoundIdent(vec![
                        ast::Ident::from_string(&alias.table),
                        ast::Ident::from_string(column),
                    ])))
                }

                exprs
            }
            other => vec![other],
        })
    }
}

#[cfg(test)]
mod tests {
    use ast::ObjectReference;
    use rayexec_bullet::datatype::DataType;

    use super::*;

    #[test]
    fn expand_none() {
        let bind_context = BindContext::new();
        let expander = SelectExprExpander::new(bind_context.root_scope_ref(), &bind_context);

        let exprs = vec![
            ast::SelectExpr::Expr(ast::Expr::Literal(ast::Literal::Number("1".to_string()))),
            ast::SelectExpr::Expr(ast::Expr::Literal(ast::Literal::Number("2".to_string()))),
        ];

        // Unchanged.
        let expected = exprs.clone();
        let expanded = expander.expand_all_select_exprs(exprs).unwrap();

        assert_eq!(expected, expanded);
    }

    #[test]
    fn expand_unqualified() {
        let mut bind_context = BindContext::new();
        bind_context
            .push_table(
                bind_context.root_scope_ref(),
                Some(TableAlias {
                    database: Some("d1".to_string()),
                    schema: Some("s1".to_string()),
                    table: "t1".to_string(),
                }),
                vec![DataType::Utf8, DataType::Utf8],
                vec!["c1".to_string(), "c2".to_string()],
            )
            .unwrap();

        let expander = SelectExprExpander::new(bind_context.root_scope_ref(), &bind_context);

        let exprs = vec![ast::SelectExpr::Wildcard(ast::Wildcard::default())];

        let expected = vec![
            ast::SelectExpr::Expr(ast::Expr::CompoundIdent(vec![
                ast::Ident::from_string("t1"),
                ast::Ident::from_string("c1"),
            ])),
            ast::SelectExpr::Expr(ast::Expr::CompoundIdent(vec![
                ast::Ident::from_string("t1"),
                ast::Ident::from_string("c2"),
            ])),
        ];
        let expanded = expander.expand_all_select_exprs(exprs).unwrap();

        assert_eq!(expected, expanded);
    }

    #[test]
    fn expand_qualified() {
        let mut bind_context = BindContext::new();
        // Add 't1'
        bind_context
            .push_table(
                bind_context.root_scope_ref(),
                Some(TableAlias {
                    database: Some("d1".to_string()),
                    schema: Some("s1".to_string()),
                    table: "t1".to_string(),
                }),
                vec![DataType::Utf8, DataType::Utf8],
                vec!["c1".to_string(), "c2".to_string()],
            )
            .unwrap();
        // Add 't2'
        bind_context
            .push_table(
                bind_context.root_scope_ref(),
                Some(TableAlias {
                    database: Some("d1".to_string()),
                    schema: Some("s1".to_string()),
                    table: "t2".to_string(),
                }),
                vec![DataType::Utf8, DataType::Utf8],
                vec!["c3".to_string(), "c4".to_string()],
            )
            .unwrap();

        let expander = SelectExprExpander::new(bind_context.root_scope_ref(), &bind_context);

        // Expand just 't1'
        let exprs = vec![ast::SelectExpr::QualifiedWildcard(
            ObjectReference(vec![ast::Ident::from_string("t1")]),
            ast::Wildcard::default(),
        )];

        let expected = vec![
            ast::SelectExpr::Expr(ast::Expr::CompoundIdent(vec![
                ast::Ident::from_string("t1"),
                ast::Ident::from_string("c1"),
            ])),
            ast::SelectExpr::Expr(ast::Expr::CompoundIdent(vec![
                ast::Ident::from_string("t1"),
                ast::Ident::from_string("c2"),
            ])),
        ];
        let expanded = expander.expand_all_select_exprs(exprs).unwrap();

        assert_eq!(expected, expanded);
    }
}
