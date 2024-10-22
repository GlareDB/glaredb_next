use std::collections::{HashMap, HashSet};

use rayexec_error::{RayexecError, Result};

use super::edge::{EdgeId, HyperEdges};
use super::estimate::CardinalityEstimator;
use crate::explain::context_display::{debug_print_context, ContextDisplayMode};
use crate::expr;
use crate::expr::column_expr::ColumnExpr;
use crate::logical::binder::bind_context::{BindContext, TableRef};
use crate::logical::logical_filter::LogicalFilter;
use crate::logical::logical_join::{
    ComparisonCondition,
    JoinType,
    LogicalArbitraryJoin,
    LogicalComparisonJoin,
    LogicalCrossJoin,
};
use crate::logical::operator::{LocationRequirement, LogicalNode, LogicalOperator, Node};
use crate::logical::statistics::StatisticsValue;
use crate::optimizer::filter_pushdown::extracted_filter::ExtractedFilter;
use crate::optimizer::join_reorder::set::{binary_partitions, powerset};

/// Unique id for identifying nodes in the graph.
pub type RelId = usize;

/// Unique id for extra filters in the graph.
pub type FilterId = usize;

/// Key for a generated plan. Made up of sorted relation ids.
// TODO: Bits
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PlanKey(pub Vec<RelId>);

impl PlanKey {
    /// Creates a new plan key from relation ids.
    ///
    /// This sorts the ids to ensure they're consistent.
    fn new_from_ids(ids: impl IntoIterator<Item = RelId>) -> Self {
        let mut v: Vec<_> = ids.into_iter().collect();
        v.sort_unstable();
        PlanKey(v)
    }

    /// Returns if this key is for a base relation.
    ///
    /// A base relation key will only have one relation id (itself).
    fn is_base(&self) -> bool {
        self.0.len() == 1
    }
}

/// A generated plan represents a either a join between two plan subsets, or a
/// base relations.
#[derive(Debug)]
pub struct GeneratedPlan {
    /// The key for this plan.
    pub key: PlanKey,
    /// Relative cost of executing _this_ plan.
    pub cost: f64,
    /// Estimated cardinality for this plan.
    pub cardinality: f64,
    /// Output table refs for this plan.
    ///
    /// Union of all child output refs.
    pub output_refs: HashSet<TableRef>,
    /// Edges containing the conditions that should be used when joining left
    /// and right.
    ///
    /// Empty when just a base relation.
    pub edges: HashSet<EdgeId>,
    /// Left input to the plan. Will be None if this plan is for a base relation.
    pub left_input: Option<PlanKey>,
    /// Right input to the plan. Will be None if this plan is for a base relation.
    pub right_input: Option<PlanKey>,
    /// Filters that will be applied to the left input.
    ///
    /// Empty when just a base relation.
    pub left_filters: HashSet<FilterId>,
    /// Filters that will be applied to the right input.
    ///
    /// Empty when just a base relation.
    pub right_filters: HashSet<FilterId>,
    /// Complete set of used edges up to and including this plan.
    ///
    /// Union of all edges used in children.
    ///
    /// This lets us track which filters/conditions we have used so far when
    /// considering this join order. We don't want to reuse filters/conditions
    /// within a join order.
    pub used: UsedEdges,
}

/// Tracks edges that have been used thus far in a particular join ordering.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UsedEdges {
    /// Complete set of filters used.
    pub filters: HashSet<FilterId>,
    /// Complete set of edges used.
    pub edges: HashSet<EdgeId>,
}

impl UsedEdges {
    /// Creates a set of used edges from two existing sets.
    fn unioned(left: &UsedEdges, right: &UsedEdges) -> Self {
        UsedEdges {
            filters: left
                .filters
                .iter()
                .chain(right.filters.iter())
                .copied()
                .collect(),
            edges: left
                .edges
                .iter()
                .chain(right.edges.iter())
                .copied()
                .collect(),
        }
    }

    fn mark_edges_used(&mut self, edges: impl IntoIterator<Item = EdgeId>) {
        self.edges.extend(edges)
    }

    fn mark_filters_used(&mut self, filters: impl IntoIterator<Item = FilterId>) {
        self.filters.extend(filters)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct UnconnectedFilter {
    pub filter: ExtractedFilter,
    pub min_ndv: f64,
}

#[derive(Debug)]
pub struct BaseRelation {
    pub operator: LogicalOperator,
    pub output_refs: HashSet<TableRef>,
    pub cardinality: f64,
}

#[derive(Debug)]
pub struct Graph {
    /// Hyper edged in the graph.
    hyper_edges: HyperEdges,
    /// Extra filters in the graph.
    filters: HashMap<FilterId, UnconnectedFilter>,
    /// Base relations in the graph that we're joining.
    base_relations: HashMap<RelId, BaseRelation>,
}

impl Graph {
    pub fn new(
        base_relations: impl IntoIterator<Item = LogicalOperator>,
        conditions: impl IntoIterator<Item = ComparisonCondition>,
        filters: impl IntoIterator<Item = ExtractedFilter>,
        bind_context: &BindContext,
    ) -> Result<Self> {
        let base_relations: HashMap<RelId, BaseRelation> = base_relations
            .into_iter()
            .map(|op| {
                let output_refs = op.get_output_table_refs().into_iter().collect();
                let cardinality = op.cardinality().value().copied().unwrap_or(20_000);

                // println!(
                //     "{output_refs:?}  {cardinality}, {}",
                //     match &op {
                //         LogicalOperator::Project(_) => "project",
                //         LogicalOperator::Filter(_) => "filter",
                //         LogicalOperator::Scan(_) => "scan",
                //         LogicalOperator::CrossJoin(_) => "cross",
                //         LogicalOperator::ComparisonJoin(_) => "cmp",
                //         LogicalOperator::ArbitraryJoin(_) => "arb",
                //         other => "other",
                //     }
                // );

                BaseRelation {
                    operator: op,
                    output_refs,
                    cardinality: cardinality as f64,
                }
            })
            .enumerate()
            .collect();

        let hyper_edges = HyperEdges::new(conditions, &base_relations)?;
        debug_print_context(ContextDisplayMode::Enriched(bind_context), &hyper_edges);

        let mut unconnected_filters: HashMap<FilterId, UnconnectedFilter> = HashMap::new();

        for (idx, filter) in filters.into_iter().enumerate() {
            let mut min_ndv = f64::MAX;

            for (_, rel) in &base_relations {
                if filter.table_refs.is_subset(&rel.output_refs) {
                    min_ndv = f64::min(min_ndv, rel.cardinality);
                }
            }

            unconnected_filters.insert(idx, UnconnectedFilter { filter, min_ndv });
        }

        Ok(Graph {
            hyper_edges,
            filters: unconnected_filters,
            base_relations,
        })
    }

    pub fn try_build(&mut self, bind_context: &BindContext) -> Result<LogicalOperator> {
        let mut plans = self.generate_plans(bind_context)?;

        // Get longest plan (plan that contains all relations).
        let key = PlanKey::new_from_ids(0..self.base_relations.len());
        let longest = plans
            .remove(&key)
            .ok_or_else(|| RayexecError::new("Missing longest generated plan"))?;

        let plan = self.build_from_generated(&longest, &mut plans)?;

        // All edges and relations should have been used to build up the plan.
        assert!(self.hyper_edges.all_edges_removed());
        assert!(self.base_relations.is_empty());

        // But we may still have filters. Apply them to the final plan.
        let filter_ids: HashSet<_> = self.filters.keys().copied().collect();
        let plan = self.apply_filters(plan, &filter_ids)?;

        assert!(self.filters.is_empty());

        Ok(plan)
    }

    fn build_from_generated(
        &mut self,
        generated: &GeneratedPlan,
        plans: &mut HashMap<PlanKey, GeneratedPlan>,
    ) -> Result<LogicalOperator> {
        // If we're building a base relation, we can just return the relation
        // as-is. No other field should have been populated.
        if generated.key.is_base() {
            assert!(generated.left_input.is_none());
            assert!(generated.right_input.is_none());
            assert!(generated.edges.is_empty());
            assert!(generated.left_filters.is_empty());
            assert!(generated.right_filters.is_empty());

            // There should always be one generated plan referencing the base
            // relation.
            let rel = self
                .base_relations
                .remove(&generated.key.0[0])
                .ok_or_else(|| RayexecError::new("Missing base relation"))?;

            return Ok(rel.operator);
        }

        // Otherwise build up the plan.

        let left_gen = plans
            .remove(generated.left_input.as_ref().expect("left to be set"))
            .ok_or_else(|| RayexecError::new("Missing left input"))?;
        let right_gen = plans
            .remove(generated.right_input.as_ref().expect("right to be set"))
            .ok_or_else(|| RayexecError::new("Missing right input"))?;

        // Swap sides if needed. We always want left (build) side to have the
        // lower cardinality (not necessarily cost).
        let plan_swap_sides = right_gen.cardinality < left_gen.cardinality;

        let left = self.build_from_generated(&left_gen, plans)?;
        let right = self.build_from_generated(&right_gen, plans)?;

        let left = self.apply_filters(left, &generated.left_filters)?;
        let right = self.apply_filters(right, &generated.right_filters)?;

        let [left, right] = if plan_swap_sides {
            [right, left]
        } else {
            [left, right]
        };

        let mut conditions = Vec::with_capacity(generated.edges.len());

        for &edge_id in &generated.edges {
            let edge = self
                .hyper_edges
                .remove_edge(edge_id)
                .ok_or_else(|| RayexecError::new("Condition already used"))?;

            // Check if we have to flip the condition (left side of the
            // condition referencing the right side of the original unswapped
            // plan).
            let condition_swap_sides = edge.left_refs.is_subset(&right_gen.output_refs);
            let mut condition = edge.condition;

            if condition_swap_sides {
                condition.flip_sides();
            }

            // Update condition if we swapped.
            if plan_swap_sides {
                condition.flip_sides();
            }

            conditions.push(condition);
        }

        if conditions.is_empty() {
            // No conditions, simple cross join.
            Ok(LogicalOperator::CrossJoin(Node {
                node: LogicalCrossJoin,
                location: LocationRequirement::Any,
                children: vec![left, right],
            }))
        } else {
            // We have conditions, create comparison join.
            Ok(LogicalOperator::ComparisonJoin(Node {
                node: LogicalComparisonJoin {
                    join_type: JoinType::Inner,
                    conditions,
                    cardinality: StatisticsValue::Estimated(generated.cardinality as usize),
                },
                location: LocationRequirement::Any,
                children: vec![left, right],
            }))
        }
    }

    /// Apply filters to a plan we're building up.
    ///
    /// Errors if any of the filters were previously used.
    fn apply_filters(
        &mut self,
        input: LogicalOperator,
        filters: &HashSet<FilterId>,
    ) -> Result<LogicalOperator> {
        if filters.is_empty() {
            // Nothing to do.
            return Ok(input);
        }

        let mut input_filters = Vec::with_capacity(filters.len());

        for filter_id in filters {
            let filter = self
                .filters
                .remove(&filter_id)
                .ok_or_else(|| RayexecError::new(format!("Filter previously used: {filter_id}")))?;

            input_filters.push(filter.filter.into_expression());
        }

        // Try to squash into underlying operator if able. Otherwise just wrap
        // in a filter.
        match input {
            LogicalOperator::Filter(filter) => {
                let filter_expr =
                    expr::and(input_filters.into_iter().chain([filter.node.filter])).unwrap();

                Ok(LogicalOperator::Filter(Node {
                    node: LogicalFilter {
                        filter: filter_expr,
                    },
                    location: filter.location,
                    children: filter.children,
                }))
            }
            LogicalOperator::ArbitraryJoin(join) if join.node.join_type == JoinType::Inner => {
                let condition =
                    expr::and(input_filters.into_iter().chain([join.node.condition])).unwrap();

                Ok(LogicalOperator::ArbitraryJoin(Node {
                    node: LogicalArbitraryJoin {
                        join_type: JoinType::Inner,
                        condition,
                    },
                    location: join.location,
                    children: join.children,
                }))
            }
            LogicalOperator::CrossJoin(join) => Ok(LogicalOperator::ArbitraryJoin(Node {
                node: LogicalArbitraryJoin {
                    join_type: JoinType::Inner,
                    condition: expr::and(input_filters).unwrap(),
                },
                location: join.location,
                children: join.children,
            })),
            other => Ok(LogicalOperator::Filter(Node {
                node: LogicalFilter {
                    filter: expr::and(input_filters).unwrap(),
                },
                location: LocationRequirement::Any,
                children: vec![other],
            })),
        }
    }

    fn generate_plans(
        &mut self,
        bind_context: &BindContext,
    ) -> Result<HashMap<PlanKey, GeneratedPlan>> {
        // Best plans generated for each group of relations.
        let mut best_plans: HashMap<PlanKey, GeneratedPlan> = HashMap::new();

        // Plans for just the base relation.
        for (&rel_id, base_rel) in &self.base_relations {
            let key = PlanKey::new_from_ids([rel_id]);
            best_plans.insert(
                key.clone(),
                GeneratedPlan {
                    key,
                    cost: 0.0,
                    cardinality: base_rel.cardinality,
                    output_refs: base_rel.output_refs.clone(),
                    edges: HashSet::new(),
                    left_input: None,
                    right_input: None,
                    left_filters: HashSet::new(),
                    right_filters: HashSet::new(),
                    used: UsedEdges::default(),
                },
            );
        }

        let rel_indices: Vec<_> = (0..self.base_relations.len()).collect();
        let rel_subsets = powerset(&rel_indices);

        for subset_size in 2..=self.base_relations.len() {
            for subset in rel_subsets
                .iter()
                .filter(|subset| subset.len() == subset_size)
            {
                let mut best_subset_plan: Option<GeneratedPlan> = None;

                // Iterate over all non-overlapping partitions for the subset,
                // trying each one and seeing if it would produce a join with
                // lower cost than the current best.
                let partitions = binary_partitions(subset);

                // Key for the plan we're generating.
                let plan_key = PlanKey::new_from_ids(subset.iter().copied());

                for (s1, s2) in partitions {
                    let s1 = PlanKey::new_from_ids(s1);
                    let s2 = PlanKey::new_from_ids(s2);

                    let p1 = best_plans.get(&s1).expect("plan to exist");
                    let p2 = best_plans.get(&s2).expect("plan to exist");

                    let mut edges = self.hyper_edges.find_edges(p1, p2);
                    // Sort descending by min_ndv to such that we consider more
                    // selective edges first (since ndv makes up the denominator
                    // when estimating cardinality).
                    edges.sort_unstable_by(|a, b| a.min_ndv.total_cmp(&b.min_ndv).reverse());

                    let mut estimator = CardinalityEstimator {
                        p1,
                        p2,
                        edges: &edges,
                        base_relations: &self.base_relations,
                    };

                    let cardinality = estimator.estimated_cardinality()?;

                    let left_filters = self.find_filters(p1);
                    let right_filters = self.find_filters(p2);

                    // Simple cost function.
                    //
                    // This is additive to ensure we fully include the cost of
                    // all other joins making up this plan.
                    let cost = cardinality + p1.cost + p2.cost;

                    if let Some(best) = &best_subset_plan {
                        if best.cost < cost {
                            // Try the next subsets.
                            continue;
                        }
                    }

                    let left_filters: HashSet<_> = left_filters.iter().map(|(&id, _)| id).collect();
                    let right_filters: HashSet<_> =
                        right_filters.iter().map(|(&id, _)| id).collect();

                    let conditions: HashSet<_> = edges.iter().map(|edge| edge.edge_id).collect();

                    let mut used = UsedEdges::unioned(&p1.used, &p2.used);
                    used.mark_edges_used(conditions.iter().copied());

                    used.mark_filters_used(left_filters.iter().copied());
                    used.mark_filters_used(right_filters.iter().copied());

                    let output_refs: HashSet<_> = p1
                        .output_refs
                        .iter()
                        .chain(&p2.output_refs)
                        .copied()
                        .collect();

                    // Friendship over with old best plan.
                    best_subset_plan = Some(GeneratedPlan {
                        key: plan_key.clone(),
                        cost,
                        cardinality,
                        output_refs,
                        edges: conditions,
                        left_input: Some(s1),
                        right_input: Some(s2),
                        left_filters,
                        right_filters,
                        used,
                    });
                }

                // Add to best plans.
                best_plans.insert(
                    plan_key,
                    best_subset_plan.expect("best subset plan to be populated"),
                );
            }
        }

        Ok(best_plans)
    }

    /// Find filters that apply fully to the given plan.
    fn find_filters(&self, plan: &GeneratedPlan) -> Vec<(&FilterId, &UnconnectedFilter)> {
        self.filters
            .iter()
            .filter(|(filter_id, filter)| {
                // Constant filter, this should be applied to the top of the
                // plan. An optimizer rule should be written to prune out
                // constant filters.
                //
                // We're currently assuming that a filter needs to be used
                // extactly once in the tree. And this check enforces that.
                if filter.filter.table_refs.is_empty() {
                    return false;
                }

                // Only consider filters not yet used.
                if plan.used.filters.contains(filter_id) {
                    return false;
                }

                // Only consider filters that apply to just the plan's table refs.
                if !filter.filter.table_refs.is_subset(&plan.output_refs) {
                    return false;
                }

                // Usable filter.
                true
            })
            .collect()
    }
}
