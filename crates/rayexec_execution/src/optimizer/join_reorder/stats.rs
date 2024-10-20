use std::collections::HashMap;

use rayexec_error::Result;

use super::graph::{Edge, EdgeId, FilterId, FoundEdge, GeneratedPlan};
use crate::expr::column_expr::ColumnExpr;
use crate::expr::comparison_expr::ComparisonOperator;
use crate::logical::binder::bind_context::BindContext;
use crate::logical::operator::{LogicalNode, LogicalOperator};
use crate::optimizer::filter_pushdown::extracted_filter::ExtractedFilter;

/// Default estimated cardinality to use for base relations if we don't have it
/// available to us.
///
/// This is arbitrary, but we need something to enable cost estimation at some
/// level. The value picked is based on intuition where if we don't have
/// statistic, we assume somewhat large cardinality such that we prefer working
/// with joins that are smaller than this.
const DEFAULT_UNKNOWN_CARDINALITY: usize = 20_000;

/// Statistics for a single plan.
#[derive(Debug)]
pub struct PlanStats {
    /// Estimated _output_ cardinality for this plan.
    pub cardinality: f64,
    /// Denominator for determining the selectivity of joining two plans.
    ///
    /// For base relations, this is 1.0.
    pub selectivity_denom: f64,
}

impl PlanStats {
    /// Creates initial plan stats for "base" relations that we'll be building
    /// our joins on top of.
    pub fn new_from_base_operator(
        op: &LogicalOperator,
        _bind_context: &BindContext,
    ) -> Result<Self> {
        // TODO: At some point we'll want to specialize these a bit more the get
        // more accurate stats from each operator.

        let cardinality = op
            .cardinality()
            .value()
            .copied()
            .unwrap_or(DEFAULT_UNKNOWN_CARDINALITY);

        Ok(PlanStats {
            cardinality: cardinality as f64,
            selectivity_denom: 1.0,
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
        // TODO: Use filters.

        let numerator = p1.stats.cardinality * p2.stats.cardinality;
        let mut denominator = p1.stats.selectivity_denom * p2.stats.selectivity_denom;

        const RANGE_SCALE: f64 = 5.0;

        // Assume num distinct values to be input cardinalities.
        let ndv = f64::min(p1.stats.cardinality, p2.stats.cardinality);

        for edge in edges {
            match edge.edge.condition.op {
                ComparisonOperator::Eq => {
                    denominator *= ndv;
                }
                ComparisonOperator::NotEq => {
                    // Do nothing, this would have very low selectivity.
                }
                ComparisonOperator::Lt
                | ComparisonOperator::Gt
                | ComparisonOperator::LtEq
                | ComparisonOperator::GtEq => {
                    denominator *= ndv;
                    // Scale to assume range comparison match 'n' (5) times more
                    // often than just eq.
                    denominator /= RANGE_SCALE
                }
            }
        }

        PlanStats {
            cardinality: numerator / denominator,
            selectivity_denom: denominator,
        }
    }
}
