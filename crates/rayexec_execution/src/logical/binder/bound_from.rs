use rayexec_error::Result;
use rayexec_parser::ast;
use std::sync::Arc;

use crate::{
    database::catalog_entry::CatalogEntry,
    logical::{
        operator::LocationRequirement,
        resolver::{
            resolve_context::ResolveContext, resolved_table::ResolvedTableOrCteReference,
            ResolvedMeta,
        },
    },
};

#[derive(Debug)]
pub struct BoundFrom {
    pub alias: String,
    pub column_aliases: Vec<String>,
    pub item: BoundFromItem,
}

#[derive(Debug)]
pub enum BoundFromItem {
    BaseTable(BoundBaseTable),
}

#[derive(Debug)]
pub struct BoundBaseTable {
    pub location: LocationRequirement,
    pub catalog: String,
    pub schema: String,
    pub entry: Arc<CatalogEntry>,
}

impl BoundFrom {
    pub fn bind(
        resolve_context: &ResolveContext,
        from: ast::FromNode<ResolvedMeta>,
    ) -> Result<BoundFrom> {
        unimplemented!()
    }

    fn bind_table(
        resolve_context: &ResolveContext,
        table: ast::FromBaseTable<ResolvedMeta>,
    ) -> Result<BoundFrom> {
        match resolve_context.tables.try_get_bound(table.reference)? {
            (ResolvedTableOrCteReference::Table(table), location) => {
                let column_aliases = table
                    .entry
                    .try_as_table_entry()?
                    .columns
                    .iter()
                    .map(|c| c.name.clone())
                    .collect();

                Ok(BoundFrom {
                    alias: table.entry.name.clone(),
                    column_aliases,
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
}
