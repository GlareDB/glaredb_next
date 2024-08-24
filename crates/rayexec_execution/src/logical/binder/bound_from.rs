use rayexec_error::{not_implemented, Result};
use rayexec_parser::ast;
use std::sync::Arc;

use crate::{
    database::catalog_entry::CatalogEntry,
    expr::Expression,
    logical::{
        operator::{JoinType, LocationRequirement},
        resolver::{
            resolve_context::ResolveContext, resolved_table::ResolvedTableOrCteReference,
            ResolvedMeta,
        },
    },
};

use super::bind_context::{BindContext, BindContextRef, CorrelatedColumn, TableScopeRef};

#[derive(Debug)]
pub struct BoundFrom {
    pub bind_ref: BindContextRef,
    pub item: BoundFromItem,
}

#[derive(Debug)]
pub enum BoundFromItem {
    BaseTable(BoundBaseTable),
    Join(BoundJoin),
}

#[derive(Debug)]
pub struct BoundBaseTable {
    pub location: LocationRequirement,
    pub catalog: String,
    pub schema: String,
    pub entry: Arc<CatalogEntry>,
}

#[derive(Debug)]
pub struct BoundJoin {
    /// Reference to binder for left side of join.
    pub left_bind_ref: BindContextRef,
    /// Bound left.
    pub left: Box<BoundFrom>,
    /// Reference to binder for right side of join.
    pub right_bind_ref: BindContextRef,
    /// Bound right.
    pub right: Box<BoundFrom>,
    /// Join type.
    pub join_type: JoinType,
    /// Expression we're joining on.
    pub condition: Expression,
    /// Columns on right side that are correlated with the left side of a join.
    pub right_correlated_columns: Vec<CorrelatedColumn>,
    /// If this is a lateral join.
    pub lateral: bool,
}

#[derive(Debug)]
pub struct FromBinder<'a> {
    pub current: BindContextRef,
    pub resolve_context: &'a ResolveContext,
}

impl<'a> FromBinder<'a> {
    pub fn new(current: BindContextRef, resolve_context: &'a ResolveContext) -> Self {
        FromBinder {
            current,
            resolve_context,
        }
    }

    pub fn bind(
        &self,
        bind_context: &mut BindContext,
        from: Option<ast::FromNode<ResolvedMeta>>,
    ) -> Result<BoundFrom> {
        unimplemented!()
    }

    fn bind_table(
        &self,
        bind_context: &mut BindContext,
        table: ast::FromBaseTable<ResolvedMeta>,
    ) -> Result<BoundFrom> {
        match self.resolve_context.tables.try_get_bound(table.reference)? {
            (ResolvedTableOrCteReference::Table(table), location) => {
                let column_types = table
                    .entry
                    .try_as_table_entry()?
                    .columns
                    .iter()
                    .map(|c| c.datatype.clone())
                    .collect();
                let column_names = table
                    .entry
                    .try_as_table_entry()?
                    .columns
                    .iter()
                    .map(|c| c.name.clone())
                    .collect();

                let _ = bind_context.push_table_scope(
                    self.current,
                    &table.entry.name,
                    column_types,
                    column_names,
                )?;

                Ok(BoundFrom {
                    bind_ref: self.current,
                    item: BoundFromItem::BaseTable(BoundBaseTable {
                        location,
                        catalog: table.catalog.clone(),
                        schema: table.schema.clone(),
                        entry: table.entry.clone(),
                    }),
                })
            }
            (ResolvedTableOrCteReference::Cte(cte_idx), _location) => {
                // TODO: Does location matter here?
                unimplemented!()
            }
        }
    }

    fn bind_join(
        &self,
        bind_context: &mut BindContext,
        join: ast::FromJoin<ResolvedMeta>,
    ) -> Result<BoundFrom> {
        // Bind left first.
        let left_idx = bind_context.new_child(self.current);
        let left =
            FromBinder::new(left_idx, self.resolve_context).bind(bind_context, Some(*join.left))?;

        // Bind right.
        //
        // The right bind context is created as a child of the left bind context
        // to easily check if this is a lateral join (distance between right and
        // left contexts == 1).
        let right_idx = bind_context.new_child(left_idx);
        let right = FromBinder::new(right_idx, self.resolve_context)
            .bind(bind_context, Some(*join.right))?;

        let right_correlated_columns = bind_context.correlated_columns(right_idx)?.clone();

        // If any column in right is correlated with left, then this is a
        // lateral join.
        let mut any_lateral = right_correlated_columns.iter().any(|c| c.outer == left_idx);

        let (conditions, using_cols) = match join.join_condition {
            ast::JoinCondition::On(exprs) => (vec![exprs], Vec::new()),
            ast::JoinCondition::Using(cols) => {
                let using_cols: Vec<_> = cols
                    .into_iter()
                    .map(|c| c.into_normalized_string())
                    .collect();
                (Vec::new(), using_cols)
            }
            ast::JoinCondition::Natural => {
                //
                unimplemented!()
            }
            ast::JoinCondition::None => (Vec::new(), Vec::new()),
        };

        let join_type = match join.join_type {
            ast::JoinType::Inner => JoinType::Inner,
            ast::JoinType::Left => JoinType::Left,
            ast::JoinType::Right => JoinType::Right,
            ast::JoinType::Cross => JoinType::Cross,
            other => not_implemented!("plan join type: {other:?}"),
        };

        // Move left and right into current context.
        bind_context.append_context(self.current, left_idx)?;
        bind_context.append_context(self.current, right_idx)?;

        let condition = {
            let _ = conditions;
            // TODO
            unimplemented!()
        };

        Ok(BoundFrom {
            bind_ref: self.current,
            item: BoundFromItem::Join(BoundJoin {
                left_bind_ref: left_idx,
                left: Box::new(left),
                right_bind_ref: right_idx,
                right: Box::new(right),
                join_type,
                condition,
                right_correlated_columns,
                lateral: any_lateral,
            }),
        })
    }
}
