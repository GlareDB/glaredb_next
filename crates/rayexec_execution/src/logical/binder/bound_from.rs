use rayexec_error::Result;
use rayexec_parser::ast;
use std::sync::Arc;

use crate::{
    database::catalog_entry::CatalogEntry,
    logical::{
        expr::LogicalExpression,
        operator::{JoinType, LocationRequirement},
        resolver::{
            resolve_context::ResolveContext, resolved_table::ResolvedTableOrCteReference,
            ResolvedMeta,
        },
    },
};

use super::bind_context::{BindContext, BindContextIdx, TableScopeIdx};

#[derive(Debug)]
pub struct BoundFrom {
    pub scope_idx: TableScopeIdx,
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
    pub left: Box<BoundFrom>,
    pub right: Box<BoundFrom>,
    pub join_type: JoinType,
    pub condition: LogicalExpression,
}

#[derive(Debug)]
pub struct FromBinder<'a> {
    pub current: BindContextIdx,
    pub resolve_context: &'a ResolveContext,
}

impl<'a> FromBinder<'a> {
    pub fn new(current: BindContextIdx, resolve_context: &'a ResolveContext) -> Self {
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

                let scope_idx = bind_context.push_table_scope(
                    self.current,
                    &table.entry.name,
                    column_types,
                    column_names,
                )?;

                Ok(BoundFrom {
                    scope_idx,
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
        // TODO: Check lateral, correlations
        let left_idx = bind_context.new_child(self.current);
        let left =
            FromBinder::new(left_idx, self.resolve_context).bind(bind_context, Some(*join.left))?;

        let right_idx = bind_context.new_child(self.current);
        let left = FromBinder::new(right_idx, self.resolve_context)
            .bind(bind_context, Some(*join.right))?;

        unimplemented!()
    }
}
