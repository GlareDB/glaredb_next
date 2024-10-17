use std::collections::{HashMap, HashSet, VecDeque};

use rayexec_error::{RayexecError, Result};

use super::equality_condition::EqualityCondition;
use crate::expr::conjunction_expr::{ConjunctionExpr, ConjunctionOperator};
use crate::expr::{self, Expression};
use crate::logical::binder::bind_context::TableRef;
use crate::logical::logical_filter::LogicalFilter;
use crate::logical::logical_join::{JoinType, LogicalComparisonJoin, LogicalCrossJoin};
use crate::logical::operator::{LocationRequirement, LogicalNode, LogicalOperator, Node};
use crate::optimizer::filter_pushdown::condition_extractor::JoinConditionExtractor;
use crate::optimizer::filter_pushdown::extracted_filter::ExtractedFilter;

#[derive(Debug, Copy, Clone, Default)]
pub struct JoinCost {
    /// Combined number of rows that make up all build sides in the join tree.
    pub build_side_rows: usize,
}

#[derive(Debug)]
pub struct JoinBuilder {
    /// Cost we've accumulated so far.
    accumulated_cost: JoinCost,
    /// Remaining equalities to use as join conditions.
    ///
    /// Ordered by equalities to use first.
    equalities: VecDeque<EqualityCondition>,
    /// Remaining filters that can be inserted at various levels of the tree.
    ///
    /// Keyed by an arbitrary value.
    filters: HashMap<usize, ExtractedFilter>,
    /// Remaining plans to use for building up the tree.
    ///
    /// Keyed by an arbitrary value.
    plans: HashMap<usize, TreeNode>,
    /// Next id to use for nodes.
    next_node_id: usize,
}

impl JoinBuilder {
    pub fn new(
        equalities: impl IntoIterator<Item = EqualityCondition>,
        filters: impl IntoIterator<Item = ExtractedFilter>,
        plans: impl IntoIterator<Item = LogicalOperator>,
    ) -> Self {
        let equalities: VecDeque<_> = equalities.into_iter().collect();
        let filters: HashMap<_, _> = filters.into_iter().enumerate().collect();
        let plans: HashMap<_, _> = plans
            .into_iter()
            .map(|plan| {
                let output_refs = plan.get_output_table_refs().into_iter().collect();
                TreeNode { plan, output_refs }
            })
            .enumerate()
            .collect();

        let next_node_id = plans.len();

        JoinBuilder {
            accumulated_cost: JoinCost::default(),
            equalities,
            filters,
            plans,
            next_node_id,
        }
    }

    pub fn get_cost(&self) -> JoinCost {
        self.accumulated_cost
    }

    fn add_cost_for_build_side(&mut self, node: &impl LogicalNode) {
        // TODO: Determine what to do here. Maybe instead of numeric values, we
        // have some sort of multiplier for exact vs unknown rows.
        const DEFAULT_VALUE: usize = 1000;

        let stats = node.get_statistics();
        let num_rows = stats.cardinality.value().unwrap_or(DEFAULT_VALUE);

        self.accumulated_cost.build_side_rows += num_rows;
    }

    /// Try to build the join tree.
    pub fn try_build(&mut self) -> Result<LogicalOperator> {
        /// Max number of times we can try to combine nodes.
        const MAX_COMBINE_STEPS: usize = 64;

        let mut step_count = 0;
        while self.try_combine_step()? {
            if step_count >= MAX_COMBINE_STEPS {
                return Err(RayexecError::new(format!(
                    "Join reorder: combine step count exceeded max: {MAX_COMBINE_STEPS}"
                )));
            }

            step_count += 1;
        }

        assert_eq!(0, self.equalities.len());
        assert_ne!(0, self.plans.len());

        let mut plan = match self.plans.len() {
            1 => {
                let (_, plan) = self.plans.drain().next().unwrap();
                plan.plan
            }
            _ => {
                // TODO: We could be a bit better here with interleaving the
                // filters where possible.
                let plans: Vec<_> = self.plans.drain().collect();
                let mut plans = plans.into_iter();
                let (_, plan) = plans.next().unwrap();
                let mut left = plan.plan;

                for (_, right) in plans {
                    self.add_cost_for_build_side(&left);

                    left = LogicalOperator::CrossJoin(Node {
                        node: LogicalCrossJoin,
                        location: LocationRequirement::Any,
                        children: vec![left, right.plan],
                    })
                }

                left
            }
        };

        // Apply any remaining filters.
        if !self.filters.is_empty() {
            let filter = expr::and(
                self.filters
                    .drain()
                    .map(|(_, filter)| filter.into_expression()),
            )
            .unwrap();

            plan = LogicalOperator::Filter(Node {
                node: LogicalFilter { filter },
                location: LocationRequirement::Any,
                children: vec![plan],
            })
        }

        Ok(plan)
    }

    /// Take a single step to try to combine two nodes into a join.
    ///
    /// Returns a bool indicating if we have more equalities and should continue
    /// to try to combine nodes.
    fn try_combine_step(&mut self) -> Result<bool> {
        let equality = match self.equalities.pop_front() {
            Some(equality) => equality,
            None => return Ok(false),
        };

        // Find nodes to join.
        //
        // We should always have nodes that we can join, the errors shouldn't happen.
        let (&left_node_id, _) = self
            .plans
            .iter()
            .find(|(_, plan)| plan.output_refs.contains(&equality.left_ref))
            .ok_or_else(|| RayexecError::new(format!("Missing node left node for {equality:?}")))?;
        let (&right_node_id, _) = self
            .plans
            .iter()
            .find(|(_, plan)| plan.output_refs.contains(&equality.right_ref))
            .ok_or_else(|| RayexecError::new(format!("Missing node left node for {equality:?}")))?;

        if left_node_id == right_node_id {
            // Equality is just a filter for an existing node.
            let node = self.plans.get_mut(&left_node_id).unwrap();
            node.push_equality_as_filter(equality);

            return Ok(true);
        }

        // Equality between two nodes.
        //
        // 1. Swap sides if needed.
        // 2. Find filters that apply to both sides.
        // 3. Create join using extracted equalities and the equality we just popped.

        let left = self.plans.remove(&left_node_id).unwrap();
        let right = self.plans.remove(&right_node_id).unwrap();

        let [mut left, mut right] = Self::maybe_swap_using_stats([left, right]);

        let left_refs: Vec<_> = left.output_refs.iter().copied().collect();
        let right_refs: Vec<_> = right.output_refs.iter().copied().collect();

        let combined_output_refs: HashSet<_> =
            left_refs.iter().chain(right_refs.iter()).copied().collect();

        // Find all filters that pertain to these two nodes.
        let filter_ids: Vec<_> = self
            .filters
            .iter()
            .filter_map(|(filter_id, filter)| {
                if filter.tables_refs.is_subset(&combined_output_refs) {
                    Some(*filter_id)
                } else {
                    None
                }
            })
            .collect();

        let mut filters = Vec::with_capacity(filter_ids.len());
        for filter_id in filter_ids {
            let filter = self.filters.remove(&filter_id).unwrap();
            filters.push(filter.into_expression());
        }

        let extractor = JoinConditionExtractor::new(&left_refs, &right_refs, JoinType::Inner);
        let mut conditions = extractor.extract(filters)?;

        // Apply left filters.
        left.push_filters(conditions.left_filter);
        // Apply right
        right.push_filters(conditions.right_filter);

        // Add cost.
        self.add_cost_for_build_side(&left.plan);

        // Do the join.
        conditions
            .comparisons
            .push(equality.into_comparision_condition());

        let mut join = LogicalOperator::ComparisonJoin(Node {
            node: LogicalComparisonJoin {
                join_type: JoinType::Inner,
                conditions: conditions.comparisons,
            },
            location: LocationRequirement::Any,
            children: vec![left.plan, right.plan],
        });

        // Append arbitrary filter if needed.
        if !conditions.arbitrary.is_empty() {
            join = LogicalOperator::Filter(Node {
                node: LogicalFilter {
                    filter: expr::and(conditions.arbitrary).unwrap(),
                },
                location: LocationRequirement::Any,
                children: vec![join],
            })
        }

        let node_id = self.next_node_id;
        self.next_node_id += 1;

        self.plans.insert(
            node_id,
            TreeNode {
                plan: join,
                output_refs: combined_output_refs,
            },
        );

        Ok(true)
    }

    fn maybe_swap_using_stats([left, right]: [TreeNode; 2]) -> [TreeNode; 2] {
        let left_stats = left.plan.get_statistics();
        let right_stats = right.plan.get_statistics();

        match (
            left_stats.cardinality.value(),
            right_stats.cardinality.value(),
        ) {
            (Some(left_size), Some(right_size)) if right_size < left_size => {
                // Swap, we want smaller on the left.
                [right, left]
            }
            _ => [left, right], // Unchanged.
        }
    }
}

/// Node in the tree.
#[derive(Debug)]
struct TreeNode {
    plan: LogicalOperator,
    output_refs: HashSet<TableRef>,
}

impl TreeNode {
    /// Push an equality condition as a filter for this node.
    ///
    /// May combine the filter with an already existing operator if possible.
    fn push_equality_as_filter(&mut self, equality: EqualityCondition) {
        match &mut self.plan {
            LogicalOperator::Filter(filter) => {
                // Combine with original filter.
                filter.node.filter.replace_with(|expr| {
                    Expression::Conjunction(ConjunctionExpr {
                        op: ConjunctionOperator::And,
                        expressions: vec![expr, equality.into_expression()],
                    })
                });
            }
            LogicalOperator::ComparisonJoin(join) if join.node.join_type == JoinType::Inner => {
                // Just add to conditions.
                join.node
                    .conditions
                    .push(equality.into_comparision_condition())
            }
            LogicalOperator::ArbitraryJoin(join) if join.node.join_type == JoinType::Inner => {
                // TODO: This coult turn the arbitrary join into a comparison
                // join. We could do the same with cross join.
                join.node.condition.replace_with(|expr| {
                    Expression::Conjunction(ConjunctionExpr {
                        op: ConjunctionOperator::And,
                        expressions: vec![expr, equality.into_expression()],
                    })
                })
            }
            _ => {
                // Otherwise just need to add as a new filter node.
                let child = self.plan.take();
                self.plan = LogicalOperator::Filter(Node {
                    node: LogicalFilter {
                        filter: equality.into_expression(),
                    },
                    location: LocationRequirement::Any,
                    children: vec![child],
                })
            }
        }
    }

    fn push_filters(&mut self, filters: impl IntoIterator<Item = Expression>) {
        for filter in filters {
            self.push_filter(filter);
        }
    }

    fn push_filter(&mut self, filter_expr: Expression) {
        match &mut self.plan {
            LogicalOperator::Filter(filter) => {
                // Combine with original filter.
                filter.node.filter.replace_with(|expr| {
                    Expression::Conjunction(ConjunctionExpr {
                        op: ConjunctionOperator::And,
                        expressions: vec![expr, filter_expr],
                    })
                });
            }
            LogicalOperator::ArbitraryJoin(join) if join.node.join_type == JoinType::Inner => {
                join.node.condition.replace_with(|expr| {
                    Expression::Conjunction(ConjunctionExpr {
                        op: ConjunctionOperator::And,
                        expressions: vec![expr, filter_expr],
                    })
                })
            }
            _ => {
                // Otherwise just need to add as a new filter node.
                let child = self.plan.take();
                self.plan = LogicalOperator::Filter(Node {
                    node: LogicalFilter {
                        filter: filter_expr,
                    },
                    location: LocationRequirement::Any,
                    children: vec![child],
                })
            }
        }
    }
}
