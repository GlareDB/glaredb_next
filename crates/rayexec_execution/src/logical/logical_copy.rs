use rayexec_bullet::field::Schema;
use rayexec_io::location::FileLocation;

use crate::{expr::Expression, functions::copy::CopyToFunction};

use super::{
    explainable::{ExplainConfig, ExplainEntry, Explainable},
    operator::LogicalNode,
};

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalCopyTo {
    /// Schema of input operator.
    ///
    /// Stored on this operator since the copy to sinks may need field names
    /// (e.g. writing out a header in csv).
    pub source_schema: Schema,
    pub location: FileLocation,
    pub copy_to: Box<dyn CopyToFunction>,
}

impl Explainable for LogicalNode<LogicalCopyTo> {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        self.annotate_explain(ExplainEntry::new("CopyTo"), conf)
    }
}
