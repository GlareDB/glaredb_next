use std::collections::{HashMap, HashSet};

use rayexec_error::{RayexecError, Result};

use crate::expr;
use crate::expr::comparison_expr::ComparisonOperator;
use crate::logical::binder::bind_context::TableRef;
use crate::logical::logical_filter::LogicalFilter;
use crate::logical::logical_join::{
    ComparisonCondition,
    JoinType,
    LogicalArbitraryJoin,
    LogicalComparisonJoin,
    LogicalCrossJoin,
};
use crate::logical::operator::{LocationRequirement, LogicalNode, LogicalOperator, Node};
use crate::logical::statistics::assumptions::{EQUALITY_SELECTIVITY, INEQUALITY_SELECTIVITY};
use crate::optimizer::filter_pushdown::extracted_filter::ExtractedFilter;
use crate::optimizer::join_reorder::set::{binary_partitions, powerset};

/// Default estimated cardinality to use for base relations if we don't have it
/// available to us.
///
/// This is arbitrary, but we need something to enable cost estimation at some
/// level. The value picked is based on intuition where if we don't have
/// statistic, we assume somewhat large cardinality such that we prefer working
/// with joins that are smaller than this.
const DEFAULT_CARDINALITY: usize = 20_000;

/// Unique id for identifying nodes in the graph.
type RelId = usize;

/// Unique id for indentifying join conditions (edges) in the graph.
type EdgeId = usize;

/// Unique id for extra filters in the graph.
type FilterId = usize;

/// Key for a generated plan. Made up of sorted relation ids.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct PlanKey(Vec<RelId>);

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
struct GeneratedPlan {
    /// The key for this plan.
    key: PlanKey,
    /// Relative cost of executing _this_ plan.
    ///
    /// For base relations, this is initialized to the estimated cardinality of
    /// the relation.
    cost: f64,
    /// Estimated _output_ cardinality for this plan.
    cardinality: f64,
    /// Output table refs for this plan.
    ///
    /// Union of all child output refs.
    output_refs: HashSet<TableRef>,
    /// Conditions that should be used when joining left and right.
    ///
    /// Empty when just a base relation.
    conditions: HashSet<EdgeId>,
    /// Left input to the plan. Will be None if this plan is for a base relation.
    left_input: Option<PlanKey>,
    /// Right input to the plan. Will be None if this plan is for a base relation.
    right_input: Option<PlanKey>,
    /// Filters that will be applied to the left input.
    ///
    /// Empty when just a base relation.
    left_filters: HashSet<FilterId>,
    /// Filters that will be applied to the right input.
    ///
    /// Empty when just a base relation.
    right_filters: HashSet<FilterId>,
    /// Complete set of used edges up to and including this plan.
    ///
    /// Union of all edges used in children.
    ///
    /// This lets us track which filters/conditions we have used so far when
    /// considering this join order. We don't want to reuse filters/conditions
    /// within a join order.
    used: UsedEdges,
}

/// Tracks edges that have been used thus far in a particular join ordering.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct UsedEdges {
    /// Complete set of filters used.
    filters: HashSet<FilterId>,
    /// Complete set of edges used.
    edges: HashSet<EdgeId>,
}

impl UsedEdges {
    /// Creates a set of used edges from two existing sets.
    fn unioned(left: &UsedEdges, right: &UsedEdges) -> Self {
        UsedEdges {
            filters: left
                .filters
                .iter()
                .copied()
                .chain(right.filters.iter().copied())
                .collect(),
            edges: left
                .edges
                .iter()
                .copied()
                .chain(right.edges.iter().copied())
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

/// Edge in the graph linking two relations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Edge {
    /// The join condition.
    pub condition: ComparisonCondition,
    /// Refs on the left side of the comparison.
    pub left_refs: HashSet<TableRef>,
    /// Refs on the right side of the comparison.
    pub right_refs: HashSet<TableRef>,
}

#[derive(Debug)]
pub struct Graph {
    /// Edges in the graph.
    edges: HashMap<EdgeId, Edge>,
    /// Extra filters in the graph.
    filters: HashMap<FilterId, ExtractedFilter>,
    /// Base relations in the graph that we're joining.
    base_relations: HashMap<RelId, LogicalOperator>,
}

impl Graph {
    pub fn try_build(&mut self) -> Result<LogicalOperator> {
        let mut plans = self.generate_plans()?;

        // Get longest plan (plan that contains all relations).
        let key = PlanKey::new_from_ids(0..self.base_relations.len());
        let longest = plans
            .remove(&key)
            .ok_or_else(|| RayexecError::new("Missing longest generated plan"))?;

        let plan = self.build_from_generated(longest, &mut plans)?;

        assert!(self.edges.is_empty());
        assert!(self.filters.is_empty());
        assert!(self.base_relations.is_empty());

        Ok(plan)
    }

    fn build_from_generated(
        &mut self,
        generated: GeneratedPlan,
        plans: &mut HashMap<PlanKey, GeneratedPlan>,
    ) -> Result<LogicalOperator> {
        // If we're building a base relation, we can just return the relation
        // as-is. No other field should have been populated.
        if generated.key.is_base() {
            assert!(generated.left_input.is_none());
            assert!(generated.right_input.is_none());
            assert!(generated.conditions.is_empty());
            assert!(generated.left_filters.is_empty());
            assert!(generated.right_filters.is_empty());

            // There should always be one generated plan referencing the base
            // relation.
            let operator = self
                .base_relations
                .remove(&generated.key.0[0])
                .ok_or_else(|| RayexecError::new("Missing base relation"))?;

            return Ok(operator);
        }

        // Otherwise build up the plan.

        let left_gen = plans
            .remove(&generated.left_input.expect("left_input to be set"))
            .ok_or_else(|| RayexecError::new("Missing left input"))?;
        let right_gen = plans
            .remove(&generated.right_input.expect("right_input to be set"))
            .ok_or_else(|| RayexecError::new("Missing right input"))?;

        // Swap sides if needed. We always want left (build) side to have the
        // lower cardinality (not necessarily cost).
        let swap_sides = right_gen.cardinality < left_gen.cardinality;

        let left = self.build_from_generated(left_gen, plans)?;
        let right = self.build_from_generated(right_gen, plans)?;

        let left = self.apply_filters(left, generated.left_filters)?;
        let right = self.apply_filters(right, generated.right_filters)?;

        let [left, right] = if swap_sides {
            [right, left]
        } else {
            [left, right]
        };

        let mut conditions = Vec::with_capacity(generated.conditions.len());

        for cond_id in generated.conditions {
            let mut condition = self
                .edges
                .remove(&cond_id)
                .ok_or_else(|| RayexecError::new("Condition already used"))?
                .condition;

            // Update condition if we swapped.
            if swap_sides {
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
        filters: HashSet<FilterId>,
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
                .ok_or_else(|| RayexecError::new("Filter previously used"))?;

            input_filters.push(filter.into_expression());
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
                location: *other.location(),
                children: vec![other],
            })),
        }
    }

    fn generate_plans(&self) -> Result<HashMap<PlanKey, GeneratedPlan>> {
        // Best plans generated for each group of relations.
        let mut best_plans: HashMap<PlanKey, GeneratedPlan> = HashMap::new();

        // Plans for just the base relation.
        for (&rel_id, base_rel) in &self.base_relations {
            let card = base_rel
                .get_statistics()
                .cardinality
                .value()
                .unwrap_or(DEFAULT_CARDINALITY) as f64;

            let key = PlanKey::new_from_ids([rel_id]);
            best_plans.insert(
                key.clone(),
                GeneratedPlan {
                    key,
                    cost: card,
                    cardinality: card,
                    output_refs: base_rel.get_output_table_refs().into_iter().collect(),
                    conditions: HashSet::new(),
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

                // Key for the plan we're generating.
                let plan_key = PlanKey::new_from_ids(subset.iter().copied());

                for (s1, s2) in partitions {
                    let s1 = PlanKey::new_from_ids(s1);
                    let s2 = PlanKey::new_from_ids(s2);

                    let p1 = best_plans.get(&s1).expect("plan to exist");
                    let p2 = best_plans.get(&s2).expect("plan to exist");

                    let conditions = self.find_conditions(p1, p2);

                    let est_cardinality = Self::estimate_output_cardinality(p1, p2, &conditions);

                    // Simple cost function.
                    //
                    // This is additive to ensure we fully include the cost of
                    // all other joins making up this plan.
                    let cost = est_cardinality + p1.cost + p2.cost;

                    if let Some(best) = &best_subset_plan {
                        if best.cost < cost {
                            // Try the next subsets.
                            continue;
                        }
                    }

                    // TODO: This should be incorporated into estimated
                    // cardinality (and cost).
                    let left_filters = self.find_filters(p1);
                    let right_filters = self.find_filters(p2);

                    let conditions: HashSet<_> = conditions.iter().map(|(&id, _)| id).collect();

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
                        cardinality: est_cardinality,
                        output_refs,
                        conditions,
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

    /// Find join conditions between plans `p1` and `p2`.
    fn find_conditions(&self, p1: &GeneratedPlan, p2: &GeneratedPlan) -> Vec<(&EdgeId, &Edge)> {
        self.edges
            .iter()
            .filter(|(edge_id, edge)| {
                // Only consider conditions not previously used.
                if p1.used.edges.contains(edge_id) || p2.used.edges.contains(edge_id) {
                    return false;
                }

                // Edge between p1 and p2.
                if edge.left_refs.is_subset(&p1.output_refs)
                    && edge.right_refs.is_subset(&p2.output_refs)
                {
                    return true;
                }

                // Edge between p2 and p1 (reversed)
                //
                // We don't need to track if we have to swap here. That'll be done
                // when we're building back up the logical plan from these generated
                // plans.
                if edge.right_refs.is_subset(&p2.output_refs)
                    && edge.right_refs.is_subset(&p1.output_refs)
                {
                    return true;
                }

                // Not a valid edge.
                false
            })
            .collect()
    }

    /// Find filters that apply fully to the given plan.
    fn find_filters(&self, plan: &GeneratedPlan) -> HashSet<FilterId> {
        self.filters
            .iter()
            .filter_map(|(filter_id, filter)| {
                // Only consider filters not yet used.
                if plan.used.filters.contains(filter_id) {
                    return None;
                }

                // Only consider filters that apply to just the plan's table refs.
                if !filter.tables_refs.is_subset(&plan.output_refs) {
                    return None;
                }

                // Usable filter.
                Some(*filter_id)
            })
            .collect()
    }

    /// Estimate the output cardinality of a join between two plans on some
    /// number of conditions.
    // TODO: Incorporate filters too.
    fn estimate_output_cardinality(
        p1: &GeneratedPlan,
        p2: &GeneratedPlan,
        edges: &[(&EdgeId, &Edge)],
    ) -> f64 {
        let mut selectivity = 1.0; // Default, if no edges, will be cross product.

        for (_, edge) in edges {
            match edge.condition.op {
                ComparisonOperator::Eq => selectivity *= EQUALITY_SELECTIVITY,
                _ => selectivity *= INEQUALITY_SELECTIVITY,
            }
        }

        selectivity * p1.cardinality * p2.cardinality
    }
}
