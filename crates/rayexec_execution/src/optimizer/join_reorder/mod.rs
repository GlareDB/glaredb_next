mod equality_condition;
mod join_builder;

use std::cmp::Ordering;
use std::collections::{HashSet, VecDeque};

use equality_condition::EqualityCondition;
use join_builder::{JoinBuilder, JoinCost};
use rayexec_error::{RayexecError, Result};

use super::filter_pushdown::extracted_filter::ExtractedFilter;
use super::filter_pushdown::split::split_conjunction;
use super::OptimizeRule;
use crate::expr::{self, Expression};
use crate::logical::binder::bind_context::{BindContext, TableRef};
use crate::logical::logical_filter::LogicalFilter;
use crate::logical::logical_join::{
    inner_join_est_cardinality,
    JoinType,
    LogicalComparisonJoin,
    LogicalCrossJoin,
};
use crate::logical::operator::{LocationRequirement, LogicalNode, LogicalOperator, Node};
use crate::logical::statistics::StatisticsCount;
use crate::optimizer::filter_pushdown::condition_extractor::JoinConditionExtractor;

/// Reorders joins in the plan.
///
/// Currently just does some reordering or filters + cross joins, but will
/// support switching join sides based on statistics eventually.
#[derive(Debug, Default)]
pub struct JoinReorder {}

impl OptimizeRule for JoinReorder {
    fn optimize(
        &mut self,
        bind_context: &mut BindContext,
        plan: LogicalOperator,
    ) -> Result<LogicalOperator> {
        let mut reorder = InnerJoinReorder::default();
        reorder.reorder(bind_context, plan)
    }
}

#[derive(Debug, Default)]
struct InnerJoinReorder {
    /// Extracted equalities that can be used for inner joins.
    equalities: Vec<EqualityCondition>,
    /// Extracted expressions that cannot be used for inner joins.
    filters: Vec<ExtractedFilter>,
    /// All plans that will be used to build up the join tree.
    child_plans: Vec<LogicalOperator>,
}

#[derive(Debug)]
struct GeneratedPlan {
    plan: LogicalOperator,
    cost: JoinCost,
}

impl InnerJoinReorder {
    fn add_expression(&mut self, expr: Expression) {
        let mut split = Vec::new();
        split_conjunction(expr, &mut split);

        for expr in split {
            match EqualityCondition::try_new(expr) {
                Ok(equality) => self.equalities.push(equality),
                Err(expr) => self.filters.push(ExtractedFilter::from_expr(expr)),
            }
        }
    }

    fn reorder(
        &mut self,
        bind_context: &mut BindContext,
        mut root: LogicalOperator,
    ) -> Result<LogicalOperator> {
        // Note that we're not matching on "magic" materialization scans as the
        // normal materialization scan should already handle the reorder within
        // the plan anyways.
        match &root {
            LogicalOperator::MaterializationScan(scan) => {
                // Start a new reorder for this materializations.
                let mut reorder = InnerJoinReorder::default();
                let mut plan = {
                    let mat = bind_context.get_materialization_mut(scan.node.mat)?;
                    std::mem::replace(&mut mat.plan, LogicalOperator::Invalid)
                };
                plan = reorder.reorder(bind_context, plan)?;

                let mat = bind_context.get_materialization_mut(scan.node.mat)?;
                mat.plan = plan;

                // Since the one or children in the plan might've switched
                // sides, we need to recompute the table refs to ensure they're
                // updated to be the correct order.
                //
                // "magic" materializations don't need to worry about this,
                // since they project out of the materialization (and the column
                // refs don't change).
                let table_refs = mat.plan.get_output_table_refs();
                mat.table_refs = table_refs.clone();

                let mut new_scan = scan.clone();
                new_scan.node.table_refs = table_refs;

                return Ok(LogicalOperator::MaterializationScan(new_scan));
            }
            LogicalOperator::Filter(_) | LogicalOperator::CrossJoin(_) => {
                self.extract_filters_and_join_children(root)?;
            }
            LogicalOperator::ComparisonJoin(join) if join.node.join_type == JoinType::Inner => {
                self.extract_filters_and_join_children(root)?;
            }
            LogicalOperator::ArbitraryJoin(join) if join.node.join_type == JoinType::Inner => {
                self.extract_filters_and_join_children(root)?;
            }
            _ => {
                // Can't extract at this node, try reordering children and
                // return.
                root.modify_replace_children(&mut |child| {
                    let mut reorder = Self::default();
                    reorder.reorder(bind_context, child)
                })?;
                return Ok(root);
            }
        }

        // Before reordering the join tree at this level, go ahead and reorder
        // nested joins that we're not able to flatten at this level.
        let mut child_plans = Vec::with_capacity(self.child_plans.len());
        for child in self.child_plans.drain(..) {
            let mut reorder = Self::default();
            let child = reorder.reorder(bind_context, child)?;
            child_plans.push(child);
        }

        let equalities = std::mem::take(&mut self.equalities);
        let filters = std::mem::take(&mut self.filters);

        const MAX_GENERATED_PLANS: usize = 8;

        let permutations =
            generate_permutations((0..equalities.len()).collect(), MAX_GENERATED_PLANS);

        let mut generated_plans = Vec::with_capacity(MAX_GENERATED_PLANS);

        for permutation in permutations {
            let mut equalities: Vec<_> = permutation
                .into_iter()
                .zip(equalities.iter().cloned())
                .collect();
            equalities.sort_unstable_by_key(|(key, _)| *key);

            let mut builder = JoinBuilder::new(
                equalities.into_iter().map(|(_, eq)| eq),
                filters.iter().cloned(),
                child_plans.iter().cloned(),
            );

            let plan = builder.try_build()?;
            let cost = builder.get_cost();

            println!("COST: {cost:?}");

            generated_plans.push(GeneratedPlan { plan, cost });
        }

        // Find the best cost.
        generated_plans.sort_unstable_by(|a, b| {
            a.cost
                .build_side_rows
                .cmp(&b.cost.build_side_rows)
                .reverse()
        });
        let best = generated_plans.pop().unwrap();

        Ok(best.plan)
    }

    fn extract_filters_and_join_children(&mut self, root: LogicalOperator) -> Result<()> {
        assert!(self.filters.is_empty());
        assert!(self.child_plans.is_empty());

        let mut queue: VecDeque<_> = [root].into_iter().collect();

        while let Some(plan) = queue.pop_front() {
            match plan {
                LogicalOperator::Filter(mut filter) => {
                    self.add_expression(filter.node.filter);
                    for child in filter.children.drain(..) {
                        queue.push_back(child);
                    }
                }
                LogicalOperator::CrossJoin(mut join) => {
                    for child in join.children.drain(..) {
                        queue.push_back(child);
                    }
                }
                LogicalOperator::ComparisonJoin(mut join) => {
                    if join.node.join_type == JoinType::Inner {
                        for condition in join.node.conditions {
                            self.add_expression(condition.into_expression());
                        }
                        for child in join.children.drain(..) {
                            queue.push_back(child);
                        }
                    } else {
                        // Nothing we can do (yet).
                        self.child_plans.push(LogicalOperator::ComparisonJoin(join))
                    }
                }
                LogicalOperator::ArbitraryJoin(mut join) => {
                    if join.node.join_type == JoinType::Inner {
                        self.add_expression(join.node.condition);
                        for child in join.children.drain(..) {
                            queue.push_back(child);
                        }
                    } else {
                        // Nothing we can do (yet).
                        self.child_plans.push(LogicalOperator::ArbitraryJoin(join))
                    }
                }
                other => self.child_plans.push(other),
            }
        }

        Ok(())
    }
}

/// Generate permutations of `v`.
fn generate_permutations(v: Vec<usize>, max_permutations: usize) -> Vec<Vec<usize>> {
    let mut result = Vec::new();
    let mut v_mut = v.clone();
    permute(&mut v_mut, 0, &mut result, max_permutations);
    result
}

fn permute(
    v: &mut Vec<usize>,
    start: usize,
    result: &mut Vec<Vec<usize>>,
    max_permutations: usize,
) {
    if result.len() == max_permutations {
        return;
    }

    if start == v.len() {
        result.push(v.clone());
        return;
    }

    for i in start..v.len() {
        v.swap(start, i);
        permute(v, start + 1, result, max_permutations);
        v.swap(start, i); // Backtrack to restore original order.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_permutations() {
        let v = vec![1, 2, 3];
        let expected = vec![
            vec![1, 2, 3],
            vec![1, 3, 2],
            vec![2, 1, 3],
            vec![2, 3, 1],
            vec![3, 2, 1],
        ];

        let got = generate_permutations(v, 5);

        assert_eq!(expected, got);
    }
}
