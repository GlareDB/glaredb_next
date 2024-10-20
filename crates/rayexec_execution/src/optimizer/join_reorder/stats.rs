use std::collections::HashMap;

use rayexec_error::Result;

use super::graph::{Edge, EdgeId, FilterId, FoundEdge, GeneratedPlan};
use crate::expr::column_expr::ColumnExpr;
use crate::expr::comparison_expr::ComparisonOperator;
use crate::logical::binder::bind_context::BindContext;
use crate::logical::operator::{LogicalNode, LogicalOperator};
use crate::logical::statistics::assumptions::DEFAULT_SELECTIVITY;
use crate::optimizer::filter_pushdown::extracted_filter::ExtractedFilter;

/// Default estimated cardinality to use for base relations if we don't have it
/// available to us.
///
/// This is arbitrary, but we need something to enable cost estimation at some
/// level. The value picked is based on intuition where if we don't have
/// statistic, we assume somewhat large cardinality such that we prefer working
/// with joins that are smaller than this.
const DEFAULT_UNKNOWN_CARDINALITY: usize = 20_000;

const DEFAULT_FILTER_SELECTIVITY: f64 = 0.1;

/// Statistics for a single plan.
#[derive(Debug)]
pub struct PlanStats {
    /// Estimated _output_ cardinality for this plan.
    pub cardinality: f64,
    /// Number of distinct values per column.
    pub column_ndv: HashMap<ColumnExpr, f64>,
}

impl PlanStats {
    /// Creates initial plan stats for "base" relations that we'll be building
    /// our joins on top of.
    pub fn new_from_base_operator(
        op: &LogicalOperator,
        bind_context: &BindContext,
    ) -> Result<Self> {
        // TODO: At some point we'll want to specialize these a bit more the get
        // more accurate stats from each operator.

        let cardinality = op
            .cardinality()
            .value()
            .copied()
            .unwrap_or(DEFAULT_UNKNOWN_CARDINALITY) as f64;

        println!("CARD: {cardinality}");

        let mut column_ndv = HashMap::new();

        // For each column, we're going to assume that the NDV is the
        // cardinality.
        //
        // This isn't really accurate, but it gets us something to base all
        // cardinality calculations on.
        for table_ref in op.get_output_table_refs() {
            let table = bind_context.get_table(table_ref)?;

            for col_idx in 0..table.num_columns() {
                column_ndv.insert(
                    ColumnExpr {
                        table_scope: table_ref,
                        column: col_idx,
                    },
                    cardinality,
                );
            }
        }

        Ok(PlanStats {
            cardinality,
            column_ndv,
        })
    }

    /// Generate new plan stats for joining two existing plans using the
    /// provided edges.
    pub fn new_plan_stats(
        p1: &GeneratedPlan,
        p2: &GeneratedPlan,
        edges: &[FoundEdge],
        left_filters: &[(&FilterId, &ExtractedFilter)],
        right_filters: &[(&FilterId, &ExtractedFilter)],
    ) -> Self {
        let mut column_ndv: HashMap<_, _> = p1
            .stats
            .column_ndv
            .iter()
            .chain(p2.stats.column_ndv.iter())
            .map(|(col, ndv)| (*col, *ndv))
            .collect();

        let left_card = Self::apply_filters(p1.stats.cardinality, left_filters, &mut column_ndv);
        let right_card = Self::apply_filters(p2.stats.cardinality, right_filters, &mut column_ndv);

        let mut denom = 1.0;

        for edge in edges {
            // TODO: Incorporate multiple columns from left or right.
            let left_col = edge.edge.left_cols.first().unwrap();
            let left_ndv = column_ndv.get(&left_col).unwrap();

            let right_col = edge.edge.right_cols.first().unwrap();
            let right_ndv = column_ndv.get(&right_col).unwrap();

            let ndv = f64::max(*left_ndv, *right_ndv);

            match edge.edge.condition.op {
                ComparisonOperator::Eq => {
                    // For equi-joins:
                    // Selectivity = 1 / max(NDV_R(A), NDV_S(A))
                    denom *= ndv
                }
                ComparisonOperator::NotEq => {
                    // For inequality joins, assume low selectivity.
                    denom *= 0.1 // Assuming 10% selectivity for !=
                }
                ComparisonOperator::Lt
                | ComparisonOperator::Gt
                | ComparisonOperator::LtEq
                | ComparisonOperator::GtEq => {
                    // For range joins, adjust selectivity. Assuming 1/3rd of
                    // the data falls into the range.
                    denom *= 1.0 / 3.0
                }
            }

            // Update ndv for columns in the join.
            let min = f64::min(*left_ndv, *right_ndv);

            column_ndv.insert(*left_col, min);
            column_ndv.insert(*right_col, min);
        }

        let cardinality = (left_card * right_card) / denom;

        PlanStats {
            cardinality,
            column_ndv,
        }
    }

    fn apply_filters(
        input_card: f64,
        filters: &[(&FilterId, &ExtractedFilter)],
        column_ndv: &mut HashMap<ColumnExpr, f64>,
    ) -> f64 {
        let mut card = input_card;

        for (_, filter) in filters {
            card *= DEFAULT_SELECTIVITY;

            for col in &filter.columns {
                let ndv = column_ndv.get_mut(col).unwrap();
                *ndv *= DEFAULT_SELECTIVITY;
            }
        }

        card
    }
}
