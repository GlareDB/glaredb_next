/// Provides a mapping between what the aggregate(s) produce, and how they get
/// mapped to the final projection from the operator.
///
/// A projection is able to reference either the output of an aggregate, or a
/// reference to the expression in the group by clause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateProjection {
    /// Indices for the final output.
    ///
    /// An index less than `agg_exprs_count` is referencing an aggregate output.
    ///
    /// An index greater or equal to `agg_exprs_count` is referencing a group by
    /// column.
    final_mapping: Vec<usize>,

    agg_exprs_count: usize,
}

impl AggregateProjection {}
