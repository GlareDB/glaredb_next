use std::sync::Arc;

use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use rayexec_bullet::datatype::DataType;

use crate::{
    database::catalog_entry::CatalogEntry, expr::Expression, functions::table::PlannedTableFunction,
};

use super::operator::LogicalNode;

#[derive(Debug, Clone, PartialEq)]
pub enum ScanSource {
    Table {
        catalog: String,
        schema: String,
        source: Arc<CatalogEntry>,
    },
    TableFunction {
        function: Box<dyn PlannedTableFunction>,
    },
    ExpressionList {
        rows: Vec<Vec<Expression>>,
    },
    View {
        catalog: String,
        schema: String,
        source: Arc<CatalogEntry>,
    },
}

/// Represents a scan from some source.
#[derive(Debug, Clone, PartialEq)]
pub struct LogicalScan {
    /// Types representing all columns from the source.
    pub types: Vec<DataType>,

    /// Names for all columns from the source.
    pub names: Vec<String>,

    /// Positional column projections.
    pub projection: Vec<usize>,

    /// Source of the scan.
    pub source: ScanSource,
}

impl Explainable for LogicalScan {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Scan")
    }
}
