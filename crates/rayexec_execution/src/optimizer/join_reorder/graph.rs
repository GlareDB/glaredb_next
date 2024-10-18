use std::collections::HashMap;

use rayexec_error::Result;

use crate::logical::operator::LogicalOperator;
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
    pub fn generate_plan(&self) -> Result<()> {
        // Best plans generated for each group of relations.
        let mut best_plans: HashMap<PlanKey, GeneratedPlan> = HashMap::new();

        // Plans for just the base relation.
        for (&rel_id, base_rel) in &self.base_relations {
            best_plans.insert(PlanKey::new_from_ids([rel_id]), GeneratedPlan { cost: 0 });
        }

        let rel_indices: Vec<_> = (0..self.base_relations.len()).collect();
        let rel_subsets = powerset(&rel_indices);

        for subset_size in 2..self.base_relations.len() {
            for subset in rel_subsets
                .iter()
                .filter(|subset| subset.len() == subset_size)
            {
                let mut best_subset_plan = None;

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

                    unimplemented!()
                }

                //
            }
        }

        unimplemented!()
    }

    /// Find the best join condition between plans `p1` and `p2`. May return
    /// None if no suitable conditions exist.
    fn find_condition(&self, p1: &GeneratedPlan, p2: &GeneratedPlan) -> Option<&ExtractedFilter> {
        unimplemented!()
    }

    /// Computes the cost of the join between `p1` and `p2` using the provided condition.
    fn compute_cost(
        p1: &GeneratedPlan,
        p2: &GeneratedPlan,
        condition: Option<&ExtractedFilter>,
    ) -> u64 {
        match condition {
            Some(condition) => {
                unimplemented!()
            }
            None => {
                // Cross join, very expensive.
                p1.cost.saturating_mul(p2.cost)
            }
        }
    }
}

#[derive(Debug)]
struct GeneratedPlan {
    /// Relative cost of executing _this_ plan.
    cost: u64,
}
