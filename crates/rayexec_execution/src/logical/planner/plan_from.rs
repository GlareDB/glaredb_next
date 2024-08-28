use rayexec_error::Result;

use crate::logical::{
    binder::{
        bind_context::BindContext,
        bind_query::bind_from::{BoundFrom, BoundFromItem},
    },
    logical_empty::LogicalEmpty,
    logical_scan::{LogicalScan, ScanSource},
    operator::{LocationRequirement, LogicalNode, LogicalOperator},
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
        match from.item {
            BoundFromItem::BaseTable(table) => {
                let mut types = Vec::new();
                let mut names = Vec::new();
                for table in self.bind_context.iter_tables(from.bind_ref)? {
                    types.extend(table.column_types.iter().cloned());
                    names.extend(table.column_names.iter().cloned());
                }

                let projection = (0..types.len()).collect();

                Ok(LogicalOperator::Scan(LogicalNode {
                    node: LogicalScan {
                        types,
                        names,
                        projection,
                        source: ScanSource::Table {
                            catalog: table.catalog,
                            schema: table.schema,
                            source: table.entry,
                        },
                    },
                    location: table.location,
                    children: Vec::new(),
                    input_table_refs: None,
                }))
            }
            BoundFromItem::Join(_) => unimplemented!(),
            BoundFromItem::Empty => Ok(LogicalOperator::Empty(LogicalNode {
                node: LogicalEmpty,
                location: LocationRequirement::Any,
                children: Vec::new(),
                input_table_refs: None,
            })),
        }
    }
}
