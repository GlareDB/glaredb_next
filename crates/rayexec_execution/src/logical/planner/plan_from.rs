use rayexec_error::Result;
use rayexec_parser::ast;

use crate::logical::{
    binder::{
        bind_context::BindContext,
        bound_from::{BoundFrom, BoundFromItem},
    },
    logical_scan::{LogicalScan, ScanSource},
    operator::{LogicalNode, LogicalOperator},
};

#[derive(Debug)]
pub struct FromPlanner<'a> {
    pub bind_context: &'a BindContext,
}

impl<'a> FromPlanner<'a> {
    pub fn new(bind_context: &'a BindContext) -> Self {
        FromPlanner { bind_context }
    }

    pub fn plan(&self, from: BoundFrom) -> Result<LogicalOperator> {
        let table_scope = self.bind_context.get_table_scope(from.scope_idx)?;

        match from.item {
            BoundFromItem::BaseTable(table) => Ok(LogicalOperator::Scan(LogicalNode {
                node: LogicalScan {
                    types: table_scope.column_types.clone(),
                    names: table_scope.column_names.clone(),
                    projection: (0..table_scope.column_names.len()).collect(),
                    source: ScanSource::Table {
                        catalog: table.catalog,
                        schema: table.schema,
                        source: table.entry,
                    },
                },
                location: table.location,
                children: Vec::new(),
                expressions: Vec::new(),
            })),
            BoundFromItem::Join(_) => unimplemented!(),
        }
    }
}
