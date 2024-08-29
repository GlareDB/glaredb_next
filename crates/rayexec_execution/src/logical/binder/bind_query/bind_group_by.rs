use crate::{
    expr::{column_expr::ColumnExpr, Expression},
    logical::{
        binder::bind_context::{BindContext, BindScopeRef, TableRef},
        resolver::{resolve_context::ResolveContext, ResolvedMeta},
    },
};
use rayexec_error::{not_implemented, RayexecError, Result};
use rayexec_parser::ast;
use std::collections::BTreeSet;

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
}

impl<'a> GroupByBinder<'a> {
    pub fn new(current: BindScopeRef, resolve_context: &'a ResolveContext) -> Self {
        GroupByBinder {
            current,
            resolve_context,
        }
    }

    pub fn bind(
        &self,
        bind_context: &mut BindContext,
        select_list: &mut SelectList,
        group_by: ast::GroupByNode<ResolvedMeta>,
    ) -> Result<BoundGroupBy> {
        let sets = GroupByWithSets::try_from_ast(group_by)?;

        let expressions = sets
            .expressions
            .into_iter()
            .map(|expr| {
                Ok(Expression::Column(
                    self.bind_to_select_list(select_list, expr)?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        let group_table = bind_context
            .new_ephemeral_table_from_expressions("__generated_group_expr", &expressions)?;

        Ok(BoundGroupBy {
            expressions,
            group_table,
            grouping_sets: sets.grouping_sets,
        })
    }

    // TODO: Duplicated with order binder.
    fn bind_to_select_list(
        &self,
        select_list: &mut SelectList,
        expr: ast::Expr<ResolvedMeta>,
    ) -> Result<ColumnExpr> {
        // Check if there's already something in the list that we're
        // referencing.
        if let Some(idx) = select_list.get_projection_reference(&expr)? {
            return Ok(ColumnExpr {
                table_scope: select_list.projections_table,
                column: idx,
            });
        }

        // Append our expression to the select list. We'll generate a column
        // expression that references this column.
        let idx = select_list.append_expression(ast::SelectExpr::Expr(expr));

        Ok(ColumnExpr {
            table_scope: select_list.projections_table,
            column: idx,
        })
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
