use std::collections::HashMap;

use rayexec_error::Result;

use super::graph::{
    BaseRelation,
    Edge,
    EdgeId,
    FilterId,
    FoundEdge,
    GeneratedPlan,
    RelId,
    UnconnectedFilter,
};
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
    pub selectivitiy_denom: f64,
}

impl PlanStats {
    /// Generate new plan stats for joining two existing plans using the
    /// provided edges.
    pub fn new_plan_stats(
        p1: &GeneratedPlan,
        p2: &GeneratedPlan,
        base_relations: &HashMap<RelId, BaseRelation>,
        edges: &[FoundEdge],
        left_filters: &[(&FilterId, &UnconnectedFilter)],
        right_filters: &[(&FilterId, &UnconnectedFilter)],
    ) -> Self {
        let mut numerator = 1.0;
        for rel_id in &p1.key.0 {
            let base = base_relations.get(rel_id).unwrap();
            numerator *= base.cardinality;
        }
        for rel_id in &p2.key.0 {
            let base = base_relations.get(rel_id).unwrap();
            numerator *= base.cardinality;
        }

        let mut left_denom = p1.stats.selectivitiy_denom;
        for (_, left_filter) in left_filters {
            left_denom *= left_filter.min_ndv;
        }

        let mut right_denom = p2.stats.selectivitiy_denom;
        for (_, right_filter) in right_filters {
            right_denom *= right_filter.min_ndv;
        }

        // let left_card = Self::apply_filters(p1.stats.cardinality, left_filters);
        // let right_card = Self::apply_filters(p2.stats.cardinality, right_filters);

        let mut denom = left_denom * right_denom;

        if let Some(edge) = edges.first() {
            match edge.edge.condition.op {
                ComparisonOperator::Eq => {
                    // =
                    denom *= edge.edge.min_ndv
                }
                ComparisonOperator::NotEq => {
                    denom *= 0.1 // Assuming 10% selectivity for !=
                }
                ComparisonOperator::Lt
                | ComparisonOperator::Gt
                | ComparisonOperator::LtEq
                | ComparisonOperator::GtEq => {
                    // For range joins, adjust selectivity. Assuming 1/3rd of
                    // the data falls into the range.
                    denom *= 3.0
                }
            }
        }

        let cardinality = numerator / denom;

        PlanStats {
            cardinality,
            selectivitiy_denom: denom,
        }
    }

    fn apply_filters(input_card: f64, filters: &[(&FilterId, &ExtractedFilter)]) -> f64 {
        let mut card = input_card;

        for (_, filter) in filters {
            // card *= DEFAULT_SELECTIVITY;

            // for col in &filter.columns {
            //     let ndv = column_ndv.get_mut(col).unwrap();
            //     *ndv *= DEFAULT_SELECTIVITY;
            // }
        }

        card
    }
}
