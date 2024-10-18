use std::collections::{HashMap, HashSet};

use rayexec_error::{RayexecError, Result};

use crate::expr::comparison_expr::ComparisonOperator;
use crate::logical::binder::bind_context::TableRef;
use crate::logical::logical_join::JoinType;
use crate::logical::operator::{LogicalNode, LogicalOperator};
use crate::optimizer::filter_pushdown::condition_extractor::JoinConditionExtractor;
use crate::optimizer::filter_pushdown::extracted_filter::ExtractedFilter;
use crate::optimizer::join_reorder::set::{binary_partitions, powerset};

/// Unique id for identifying nodes in the graph.
type RelId = usize;

/// Unique id for indentifying join conditions (edges) in the graph.
type ConditionId = usize;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct PlanKey(Vec<RelId>);

impl PlanKey {
    fn new_from_ids(ids: impl IntoIterator<Item = RelId>) -> Self {
        let mut v: Vec<_> = ids.into_iter().collect();
        v.sort_unstable();
        PlanKey(v)
    }
}

#[derive(Debug)]
pub struct Graph {
    /// Extracted join conditions.
    conditions: HashMap<ConditionId, ExtractedFilter>,
    /// Base relations in the graph that we're joining.
    base_relations: HashMap<RelId, LogicalOperator>,
}

impl Graph {
    fn generate_plan(&self) -> Result<GeneratedPlan> {
        // Best plans generated for each group of relations.
        let mut best_plans: HashMap<PlanKey, GeneratedPlan> = HashMap::new();

        // Plans for just the base relation.
        for (&rel_id, base_rel) in &self.base_relations {
            best_plans.insert(
                PlanKey::new_from_ids([rel_id]),
                GeneratedPlan {
                    cost: 0,
                    output_refs: base_rel.get_output_table_refs().into_iter().collect(),
                },
            );
        }

        let rel_indices: Vec<_> = (0..self.base_relations.len()).collect();
        let rel_subsets = powerset(&rel_indices);

        for subset_size in 2..self.base_relations.len() {
            for subset in rel_subsets
                .iter()
                .filter(|subset| subset.len() == subset_size)
            {
                let mut best_subset_plan: Option<GeneratedPlan> = None;

                // Iterate over all non-overlapping partitions for the subset,
                // trying each one and seeing if it would produce a join with
                // lower cost than the current best.
                let partitions = binary_partitions(subset);

                for (s1, s2) in partitions {
                    let s1 = PlanKey::new_from_ids(s1);
                    let s2 = PlanKey::new_from_ids(s2);

                    let p1 = best_plans.get(&s1).expect("plan to exist");
                    let p2 = best_plans.get(&s2).expect("plan to exist");

                    let condition = self.find_condition(p1, p2);

                    let cost = Self::try_compute_cost(p1, p2, condition)?;

                    if let Some(best) = &best_subset_plan {
                        if best.cost < cost {
                            // Try the next subsets.
                            continue;
                        }
                    }

                    best_subset_plan = Some(GeneratedPlan {
                        cost,
                        output_refs: p1
                            .output_refs
                            .iter()
                            .copied()
                            .chain(p2.output_refs.iter().copied())
                            .collect(),
                    });
                }

                // Add to best plans.
                let plan_key = PlanKey::new_from_ids(subset.iter().copied());
                best_plans.insert(
                    plan_key,
                    best_subset_plan.expect("best subset plan to be populated"),
                );
            }
        }

        let plan_key = PlanKey::new_from_ids(self.base_relations.keys().copied());
        let plan = best_plans
            .remove(&plan_key)
            .ok_or_else(|| RayexecError::new("Missing final best plan"))?;

        Ok(plan)
    }

    /// Find the best join condition between plans `p1` and `p2`. May return
    /// None if no suitable conditions exist.
    fn find_condition(&self, p1: &GeneratedPlan, p2: &GeneratedPlan) -> Option<&ExtractedFilter> {
        unimplemented!()
    }

    /// Computes the cost of the join between `p1` and `p2` using the provided condition.
    fn try_compute_cost(
        p1: &GeneratedPlan,
        p2: &GeneratedPlan,
        condition: Option<&ExtractedFilter>,
    ) -> Result<u64> {
        match condition {
            Some(condition) => {
                const EQ_COST_FACTOR: u64 = 1;
                const CMP_COST_FACTOR: u64 = 5;
                const DEFAULT_COST_FACTOR: u64 = 10;

                let extractor =
                    JoinConditionExtractor::new(&p1.output_refs, &p2.output_refs, JoinType::Inner);

                let scale = match extractor.try_get_comparison_operator(&condition.filter)? {
                    Some(ComparisonOperator::Eq) => EQ_COST_FACTOR,
                    Some(_) => CMP_COST_FACTOR,
                    None => DEFAULT_COST_FACTOR,
                };

                let cost = (p1.cost + p2.cost) * scale;

                Ok(cost)
            }
            None => {
                // Cross join, very expensive.
                let cost = p1.cost.saturating_mul(p2.cost);

                Ok(cost)
            }
        }
    }
}

#[derive(Debug)]
struct GeneratedPlan {
    /// Relative cost of executing _this_ plan.
    cost: u64,
    /// Output table refs for this plan.
    output_refs: HashSet<TableRef>,
}
