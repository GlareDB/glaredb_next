use crate::{
    expr::{column_expr::ColumnExpr, literal_expr::LiteralExpr, Expression},
    logical::{
        binder::{
            bind_context::{BindContext, BindScopeRef, TableRef},
            expr_binder::{ExpressionBinder, RecursionContext},
        },
        resolver::{resolve_context::ResolveContext, ResolvedMeta},
    },
};
use rayexec_bullet::scalar::ScalarValue;
use rayexec_error::{not_implemented, RayexecError, Result};
use rayexec_parser::ast;
use std::collections::{BTreeSet, HashSet};

use super::select_list::SelectList;

#[derive(Debug, Clone, PartialEq)]
pub struct BoundGroupBy {
    pub expressions: Vec<Expression>,
    pub group_table: TableRef,
    pub grouping_sets: Vec<BTreeSet<usize>>,
}

#[derive(Debug)]
pub struct GroupByBinder<'a> {
    pub current: BindScopeRef,
    pub resolve_context: &'a ResolveContext,
    /// Set of columns in the select list that we've already bound a GROUP BY
    /// for.
    ///
    /// This is used deduplicate GROUP BY expressions if multiple expressions
    /// reference the same select list column.
    pub referenced_select_exprs: HashSet<ColumnExpr>,
}

impl<'a> GroupByBinder<'a> {
    pub fn new(current: BindScopeRef, resolve_context: &'a ResolveContext) -> Self {
        GroupByBinder {
            current,
            resolve_context,
            referenced_select_exprs: HashSet::new(),
        }
    }

    pub fn bind(
        &mut self,
        bind_context: &mut BindContext,
        select_list: &mut SelectList,
        group_by: ast::GroupByNode<ResolvedMeta>,
    ) -> Result<BoundGroupBy> {
        let sets = GroupByWithSets::try_from_ast(group_by)?;

        let group_table = bind_context.new_ephemeral_table()?;
        let expressions = sets
            .expressions
            .into_iter()
            .map(|expr| self.bind_expr(bind_context, select_list, group_table, expr))
            .collect::<Result<Vec<_>>>()?;

        Ok(BoundGroupBy {
            expressions,
            group_table,
            grouping_sets: sets.grouping_sets,
        })
    }

    /// Binds an expression in the GROUP BY clause.
    ///
    /// First tries to bind to the select list. If successfully bound, the
    /// expression in the select list will be modifed to point to the expression
    /// in the GROUP BY clause.
    ///
    /// Otherwise just tries to bind the expression normally.
    fn bind_expr(
        &mut self,
        bind_context: &mut BindContext,
        select_list: &mut SelectList,
        group_table: TableRef,
        expr: ast::Expr<ResolvedMeta>,
    ) -> Result<Expression> {
        // Check if there's already something in the list that we're
        // referencing.
        match select_list.column_expr_for_reference(bind_context, &expr)? {
            Some(col_expr) => {
                println!("COL EXPR: {col_expr}");

                if self.referenced_select_exprs.contains(&col_expr) {
                    // Another expression in the GROUP BY is already referencing
                    // this column, can just replace with a constant.
                    return Ok(Expression::Literal(LiteralExpr {
                        literal: ScalarValue::Int8(1),
                    }));
                }

                // Swap expression in select list with a reference that points
                // to the GROUP BY expression.

                let datatype = col_expr.datatype(bind_context)?;
                let idx = bind_context.push_column_for_table(
                    group_table,
                    "__generated_group_expr",
                    datatype,
                )?;

                let select_expr = select_list.get_projection_mut(col_expr.column)?;
                let orig = std::mem::replace(
                    select_expr,
                    Expression::Column(ColumnExpr {
                        table_scope: group_table,
                        column: idx,
                    }),
                );

                self.referenced_select_exprs.insert(col_expr);

                Ok(orig)
            }
            None => {
                let expr = ExpressionBinder::new(self.current, self.resolve_context)
                    .bind_expression(
                        bind_context,
                        &expr,
                        RecursionContext {
                            allow_window: false,
                            allow_aggregate: false,
                        },
                    )?;

                let datatype = expr.datatype(bind_context)?;
                bind_context.push_column_for_table(
                    group_table,
                    "__generated_group_expr",
                    datatype,
                )?;

                Ok(expr)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct GroupByWithSets {
    expressions: Vec<ast::Expr<ResolvedMeta>>,
    grouping_sets: Vec<BTreeSet<usize>>,
}

impl GroupByWithSets {
    fn try_from_ast(group_by: ast::GroupByNode<ResolvedMeta>) -> Result<Self> {
        match group_by {
            ast::GroupByNode::All => not_implemented!("GROUP BY ALL"),
            ast::GroupByNode::Exprs { mut exprs } => {
                let expr = match exprs.len() {
                    1 => exprs.pop().unwrap(),
                    _ => return Err(RayexecError::new("Invalid number of group by expressions")),
                };

                let (expressions, grouping_sets) = match expr {
                    ast::GroupByExpr::Expr(exprs) => {
                        let len = exprs.len();
                        (exprs, vec![(0..len).collect()])
                    }
                    ast::GroupByExpr::Rollup(exprs) => {
                        let len = exprs.len();
                        let mut sets: Vec<_> = (0..len).map(|i| (0..(len - i)).collect()).collect();
                        sets.push(BTreeSet::new()); // Empty set.
                        (exprs, sets)
                    }
                    ast::GroupByExpr::Cube(exprs) => {
                        let len = exprs.len();
                        let mut sets = Vec::new();

                        // Powerset
                        for mask in 0..(1 << len) {
                            let mut set = BTreeSet::new();
                            let mut bitset = mask;
                            while bitset > 0 {
                                let right = bitset & !(bitset - 1) as u64;
                                let idx = right.trailing_zeros() as usize;
                                set.insert(idx);
                                bitset &= bitset - 1;
                            }
                            sets.push(set);
                        }

                        (exprs, sets)
                    }
                    ast::GroupByExpr::GroupingSets(_exprs) => {
                        not_implemented!("GROUPING SETS")
                    }
                };

                Ok(GroupByWithSets {
                    expressions,
                    grouping_sets,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_by_with_sets_from_group_by_single() {
        // GROUP BY a
        let node = ast::GroupByNode::Exprs {
            exprs: vec![ast::GroupByExpr::Expr(vec![ast::Expr::Ident(
                ast::Ident::from_string("a"),
            )])],
        };

        let sets = GroupByWithSets::try_from_ast(node).unwrap();
        let expected = GroupByWithSets {
            expressions: vec![ast::Expr::Ident(ast::Ident::from_string("a"))],
            grouping_sets: vec![[0].into()],
        };

        assert_eq!(expected, sets)
    }

    #[test]
    fn group_by_with_sets_from_group_by_many() {
        // GROUP BY a, b
        let node = ast::GroupByNode::Exprs {
            exprs: vec![ast::GroupByExpr::Expr(vec![
                ast::Expr::Ident(ast::Ident::from_string("a")),
                ast::Expr::Ident(ast::Ident::from_string("b")),
            ])],
        };

        let sets = GroupByWithSets::try_from_ast(node).unwrap();
        let expected = GroupByWithSets {
            expressions: vec![
                ast::Expr::Ident(ast::Ident::from_string("a")),
                ast::Expr::Ident(ast::Ident::from_string("b")),
            ],
            grouping_sets: vec![[0, 1].into()],
        };

        assert_eq!(expected, sets)
    }

    #[test]
    fn group_by_with_sets_from_rollup() {
        // GROUP BY ROLLUP a, b, c
        let node = ast::GroupByNode::Exprs {
            exprs: vec![ast::GroupByExpr::Rollup(vec![
                ast::Expr::Ident(ast::Ident::from_string("a")),
                ast::Expr::Ident(ast::Ident::from_string("b")),
                ast::Expr::Ident(ast::Ident::from_string("c")),
            ])],
        };

        let sets = GroupByWithSets::try_from_ast(node).unwrap();
        let expected = GroupByWithSets {
            expressions: vec![
                ast::Expr::Ident(ast::Ident::from_string("a")),
                ast::Expr::Ident(ast::Ident::from_string("b")),
                ast::Expr::Ident(ast::Ident::from_string("c")),
            ],
            grouping_sets: vec![[0, 1, 2].into(), [0, 1].into(), [0].into(), [].into()],
        };

        assert_eq!(expected, sets)
    }

    #[test]
    fn group_by_with_sets_from_cube() {
        // GROUP BY CUBE a, b, c
        let node = ast::GroupByNode::Exprs {
            exprs: vec![ast::GroupByExpr::Cube(vec![
                ast::Expr::Ident(ast::Ident::from_string("a")),
                ast::Expr::Ident(ast::Ident::from_string("b")),
                ast::Expr::Ident(ast::Ident::from_string("c")),
            ])],
        };

        let sets = GroupByWithSets::try_from_ast(node).unwrap();
        let expected = GroupByWithSets {
            expressions: vec![
                ast::Expr::Ident(ast::Ident::from_string("a")),
                ast::Expr::Ident(ast::Ident::from_string("b")),
                ast::Expr::Ident(ast::Ident::from_string("c")),
            ],
            grouping_sets: vec![
                [].into(),
                [0].into(),
                [1].into(),
                [0, 1].into(),
                [2].into(),
                [0, 2].into(),
                [1, 2].into(),
                [0, 1, 2].into(),
            ],
        };

        assert_eq!(expected, sets)
    }
}
