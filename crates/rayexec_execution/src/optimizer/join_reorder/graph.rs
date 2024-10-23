use std::collections::{HashMap, HashSet};

use rayexec_error::{RayexecError, Result};

use super::edge::{EdgeId, HyperEdges, NeighborEdge};
use super::subgraph::Subgraph;
use crate::explain::context_display::{debug_print_context, ContextDisplayMode};
use crate::expr::column_expr::ColumnExpr;
use crate::expr::{self, Expression};
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
use crate::logical::statistics::assumptions::DEFAULT_SELECTIVITY;
use crate::logical::statistics::StatisticsValue;
use crate::optimizer::filter_pushdown::extracted_filter::ExtractedFilter;
use crate::optimizer::join_reorder::set::{binary_partitions, powerset};

/// Unique id for identifying nodes in the graph.
pub type RelId = usize;

/// Unique id for extra filters in the graph.
pub type FilterId = usize;
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

#[derive(Debug)]
pub struct BaseRelation {
    pub operator: LogicalOperator,
    pub output_refs: HashSet<TableRef>,
    pub cardinality: f64,
}

#[derive(Debug, Clone)]
pub struct JoinNode {
    pub set: RelationSet,
    pub cost: f64,
    pub left: RelationSet,
    pub right: RelationSet,
    pub subgraph: Subgraph,
    /// Output table refs for this plan.
    ///
    /// Union of all child output refs.
    pub output_refs: HashSet<TableRef>,
    /// Edges containing the conditions that should be used when joining left
    /// and right.
    ///
    /// Empty when just a base relation.
    pub edges: HashSet<EdgeId>,
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RelationSet {
    pub relation_indices: Vec<usize>,
}

impl RelationSet {
    fn new(indices: impl IntoIterator<Item = usize>) -> Self {
        let mut indices: Vec<_> = indices.into_iter().collect();
        indices.sort_unstable();

        RelationSet {
            relation_indices: indices,
        }
    }

    fn empty() -> Self {
        RelationSet {
            relation_indices: Vec::new(),
        }
    }

    fn base(idx: usize) -> Self {
        RelationSet {
            relation_indices: vec![idx],
        }
    }

    fn is_base(&self) -> bool {
        self.relation_indices.len() == 1
    }

    fn union(left: &RelationSet, right: &RelationSet) -> Self {
        let mut indices: Vec<_> = left
            .relation_indices
            .iter()
            .chain(right.relation_indices.iter())
            .copied()
            .collect();
        indices.sort_unstable();
        indices.dedup();

        RelationSet {
            relation_indices: indices,
        }
    }

    fn get_all_neighbor_sets(mut neighbors: Vec<usize>) -> Vec<RelationSet> {
        fn add_supersets(current: &[HashSet<usize>], neighbors: &[usize]) -> Vec<HashSet<usize>> {
            let mut added = Vec::new();

            for neighbor_set in current {
                // Find the maximum value in the current neighbor set
                let max = neighbor_set.iter().max().unwrap();
                for &neighbor in neighbors {
                    if *max >= neighbor {
                        continue;
                    }
                    if !neighbor_set.contains(&neighbor) {
                        // Create a new set by adding the neighbor.
                        let mut new_set = neighbor_set.clone();
                        new_set.insert(neighbor);
                        added.push(new_set);
                    }
                }
            }

            added
        }

        let mut sets = Vec::new();
        neighbors.sort();
        let mut added = Vec::new();

        // Initialize with sets containing each neighbor individually
        for &neighbor in &neighbors {
            let mut set = HashSet::new();
            set.insert(neighbor);
            added.push(set.clone());
            sets.push(set);
        }

        // Generate all supersets
        while !added.is_empty() {
            added = add_supersets(&added, &neighbors);
            for d in &added {
                sets.push(d.clone());
            }
        }

        sets.into_iter()
            .map(|indices| RelationSet::new(indices))
            .collect()
    }
}

#[derive(Debug)]
pub struct Graph {
    hyper_edges: HyperEdges,
    filters: HashMap<FilterId, ExtractedFilter>,
    base_relations: HashMap<RelId, BaseRelation>,
    /// Best join node plans we've found for the given set of relations.
    best_plans: HashMap<RelationSet, JoinNode>,
}

impl Graph {
    pub fn try_new(
        base_ops: impl IntoIterator<Item = LogicalOperator>,
        conditions: impl IntoIterator<Item = ComparisonCondition>,
        filters: impl IntoIterator<Item = ExtractedFilter>,
    ) -> Result<Self> {
        let base_relations: HashMap<RelId, BaseRelation> = base_ops
            .into_iter()
            .map(|op| {
                let output_refs = op.get_output_table_refs().into_iter().collect();
                let cardinality = op.cardinality().value().copied().unwrap_or(20_000) as f64;

                BaseRelation {
                    operator: op,
                    output_refs,
                    cardinality,
                }
            })
            .enumerate()
            .collect();

        let hyper_edges = HyperEdges::new(conditions, &base_relations)?;
        let filters = filters.into_iter().enumerate().collect();

        let mut best_plans = HashMap::with_capacity(base_relations.len());

        for (&rel_id, base_rel) in &base_relations {
            // Initial subgraph, we're selecting everything from the base
            // relation.
            let subgraph = Subgraph {
                numerator: base_rel.cardinality,
                selectivity_denom: 1.0,
            };

            let rel_set = RelationSet::base(rel_id);

            let node = JoinNode {
                set: rel_set.clone(),
                cost: 0.0,
                left: RelationSet::empty(),
                right: RelationSet::empty(),
                subgraph,
                output_refs: base_rel.output_refs.clone(),
                edges: HashSet::new(),
                left_filters: HashSet::new(),
                right_filters: HashSet::new(),
                used: UsedEdges::default(),
            };

            best_plans.insert(rel_set, node);
        }

        Ok(Graph {
            hyper_edges,
            filters,
            best_plans,
            base_relations,
        })
    }

    pub fn try_build(mut self) -> Result<LogicalOperator> {
        self.solve()?;

        let longest_set = RelationSet::new(0..self.base_relations.len());
        let longest = self
            .best_plans
            .remove(&longest_set)
            .ok_or_else(|| RayexecError::new("Missing longest best plan"))?;

        let plan = self.build_from_generated(&longest)?;

        // All base relations and edges should have been used to build up the
        // plan.
        assert!(self.base_relations.is_empty());
        assert!(self.hyper_edges.all_edges_removed());

        // But we may still have filters. Apply them to the final plan.
        let filter_ids: HashSet<_> = self.filters.keys().copied().collect();
        let plan = self.apply_filters(plan, &filter_ids)?;

        assert!(self.filters.is_empty());

        Ok(plan)
    }

    fn solve(&mut self) -> Result<()> {
        // Iterate over base relations to produce possible pairs.
        for base_idx in (0..self.base_relations.len()).rev() {
            let base_rel = RelationSet::base(base_idx);

            // Emit start.
            self.emit_connected_subgraphs(&base_rel)?;

            // Exclude all other base relations less than this relation's index.
            let exclude: HashSet<_> = (0..base_idx).collect();

            // Search for connected subgraphs starting from this relation.
            self.enumerate_connected_subgraphs_rec(&base_rel, &exclude)?;
        }

        Ok(())
    }

    fn enumerate_connected_subgraphs_rec(
        &mut self,
        set: &RelationSet,
        exclude: &HashSet<usize>,
    ) -> Result<()> {
        let neighbors = self.hyper_edges.find_neighbors(set, exclude);
        if neighbors.is_empty() {
            return Ok(());
        }

        let neighbor_sets = RelationSet::get_all_neighbor_sets(neighbors.clone());
        let mut combined_sets = Vec::with_capacity(neighbor_sets.len());

        for neigbor_set in neighbor_sets {
            let combined = RelationSet::union(set, &neigbor_set);
            if self.best_plans.contains_key(&combined) {
                self.emit_connected_subgraphs(&combined)?;
            }

            combined_sets.push(combined);
        }

        let mut exclude = exclude.clone();
        exclude.extend(neighbors);

        for combined in combined_sets {
            self.enumerate_connected_subgraphs_rec(&combined, &exclude)?;
        }

        Ok(())
    }

    fn emit_connected_subgraphs(&mut self, set: &RelationSet) -> Result<()> {
        // Create an exclusion set for all relations "previous" to this set, as
        // well as all relations within this set.
        let mut exclude: HashSet<_> = (0..set.relation_indices[0]).collect();
        for idx in &set.relation_indices {
            exclude.insert(*idx);
        }

        let mut neighbors = self.hyper_edges.find_neighbors(set, &exclude);
        neighbors.sort_unstable_by(|a, b| a.cmp(b).reverse());

        // Add neighbors to exlusion set, as we're going to be working them in
        // the recursive call.
        exclude.extend(&neighbors);

        for neighbor in neighbors {
            // Find edges between this node and neighbor.
            let neighbor_set = RelationSet::base(neighbor);
            let edges = self.hyper_edges.find_edges(set, &neighbor_set);

            if !edges.is_empty() {
                // We have a connection.
                self.emit_pair(set, &neighbor_set, edges)?;
            }

            // Kick off recursively visiting neighbors.
            self.enumerate_connected_complement_rec(set, &neighbor_set, &exclude)?;

            exclude.remove(&neighbor);
        }

        Ok(())
    }

    fn enumerate_connected_complement_rec(
        &mut self,
        left: &RelationSet,
        right: &RelationSet,
        exclude: &HashSet<usize>,
    ) -> Result<()> {
        let neighbors = self.hyper_edges.find_neighbors(right, &exclude);
        if neighbors.is_empty() {
            return Ok(());
        }

        let neighbor_sets = RelationSet::get_all_neighbor_sets(neighbors.clone());
        let mut combined_sets = Vec::with_capacity(neighbor_sets.len());

        for neigbor_set in neighbor_sets {
            let combined = RelationSet::union(right, &neigbor_set);
            if self.best_plans.contains_key(&combined) {
                let edges = self.hyper_edges.find_edges(left, &combined);

                if !edges.is_empty() {
                    self.emit_pair(left, &combined, edges)?;
                }
            }

            combined_sets.push(combined);
        }

        // Extend exclusion to include neighbors we just visited.
        let mut exclude = exclude.clone();
        exclude.extend(neighbors);

        // Recurse into the combined neighborder sets.
        for combined in combined_sets {
            self.enumerate_connected_complement_rec(left, &combined, &exclude)?;
        }

        Ok(())
    }

    fn emit_pair(
        &mut self,
        left: &RelationSet,
        right: &RelationSet,
        edges: Vec<NeighborEdge>,
    ) -> Result<()> {
        let left = self
            .best_plans
            .get_key_value(left)
            .ok_or_else(|| RayexecError::new("missing best plan for left"))?;
        let right = self
            .best_plans
            .get_key_value(right)
            .ok_or_else(|| RayexecError::new("missing best plan for right"))?;

        let new_set = RelationSet::union(left.0, right.0);

        let left_filters = self.find_filters(left.1);
        let right_filters = self.find_filters(right.1);

        // Clone the left subgraph, and modify it to account for the
        // joins with the right based on edges.
        let mut subgraph = left.1.subgraph;
        subgraph.update_numerator(&right.1.subgraph);

        for _ in 0..left_filters.len() + right_filters.len() {
            subgraph.numerator *= DEFAULT_SELECTIVITY;
            // subgraph.selectivity_denom *= (1.0 - DEFAULT_SELECTIVITY);
        }

        if let Some(edge) = edges.first() {
            subgraph.update_denom(&right.1.subgraph, edge);
        }

        // Get the estimated cardinality at this point in the
        // subgraph construction.
        let cardinality = subgraph.estimated_cardinality();

        // Simple cost function.
        //
        // This is additive to ensure we fully include the cost of
        // all other joins making up this plan.
        let cost = cardinality + left.1.cost + right.1.cost;

        println!("COST: {cost},       CARD: {cardinality}");

        // Check to see if this cost is lower than existing cost. Returns early
        // if not.
        match self.best_plans.get(&new_set) {
            Some(existing) => {
                if existing.cost < cost {
                    return Ok(());
                }
            }
            _ => (),
        }

        // New node is better. Create it and insert into plans.

        let left_filters: HashSet<_> = left_filters.iter().map(|(&id, _)| id).collect();
        let right_filters: HashSet<_> = right_filters.iter().map(|(&id, _)| id).collect();

        let edges: HashSet<_> = edges.iter().map(|edge| edge.edge_id).collect();

        let mut used = UsedEdges::unioned(&left.1.used, &right.1.used);
        used.mark_edges_used(edges.iter().copied());

        used.mark_filters_used(left_filters.iter().copied());
        used.mark_filters_used(right_filters.iter().copied());

        let output_refs: HashSet<_> = left
            .1
            .output_refs
            .iter()
            .chain(&right.1.output_refs)
            .copied()
            .collect();

        self.best_plans.insert(
            new_set.clone(),
            JoinNode {
                set: new_set,
                cost,
                left: left.0.clone(),
                right: right.0.clone(),
                subgraph,
                output_refs,
                edges,
                left_filters,
                right_filters,
                used,
            },
        );

        Ok(())
    }

    /// Find filters that apply fully to the given plan.
    fn find_filters(&self, node: &JoinNode) -> Vec<(&FilterId, &ExtractedFilter)> {
        self.filters
            .iter()
            .filter(|(filter_id, filter)| {
                // Constant filter, this should be applied to the top of the
                // plan. An optimizer rule should be written to prune out
                // constant filters.
                //
                // We're currently assuming that a filter needs to be used
                // extactly once in the tree. And this check enforces that.
                if filter.table_refs.is_empty() {
                    return false;
                }

                // Only consider filters not yet used.
                if node.used.filters.contains(filter_id) {
                    return false;
                }

                // Only consider filters that apply to just the plan's table refs.
                if !filter.table_refs.is_subset(&node.output_refs) {
                    return false;
                }

                // Usable filter.
                true
            })
            .collect()
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

            input_filters.push(filter.filter);
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

    fn build_from_generated(&mut self, node: &JoinNode) -> Result<LogicalOperator> {
        // If we're building a base relation, we can just return the relation
        // as-is. No other field should have been populated.
        if node.set.is_base() {
            assert!(node.edges.is_empty());

            // There should always be one generated plan referencing the base
            // relation.
            let rel = self
                .base_relations
                .remove(&node.set.relation_indices[0])
                .ok_or_else(|| RayexecError::new("Missing base relation"))?;

            return Ok(rel.operator);
        }

        // Otherwise build up the plan.

        let left_gen = self
            .best_plans
            .remove(&node.left)
            .ok_or_else(|| RayexecError::new("Missing left input"))?;
        let right_gen = self
            .best_plans
            .remove(&node.right)
            .ok_or_else(|| RayexecError::new("Missing right input"))?;

        let mut conditions = Vec::with_capacity(node.edges.len());
        for &edge_id in &node.edges {
            let edge = self
                .hyper_edges
                .remove_edge(edge_id)
                .ok_or_else(|| RayexecError::new("Edge already used"))?;

            let mut condition = edge.filter;

            let condition_swap_sides = edge.left_refs.is_subset(&right_gen.output_refs);
            if condition_swap_sides {
                condition.flip_sides();
            }

            conditions.push(condition);
        }

        // Determine if we should swap sides. We always want left (build) side
        // to have the lower cardinality (not necessarily cost).
        //
        // Don't swap sides yet, still need to apply filters.
        let plan_swap_sides =
            right_gen.subgraph.estimated_cardinality() < left_gen.subgraph.estimated_cardinality();

        let left = self.build_from_generated(&left_gen)?;
        let right = self.build_from_generated(&right_gen)?;

        let left = self.apply_filters(left, &node.left_filters)?;
        let right = self.apply_filters(right, &node.right_filters)?;

        let [left, right] = if plan_swap_sides {
            [right, left]
        } else {
            [left, right]
        };

        // If we swapped sides, we'll need to flip the join conditions to match.
        if plan_swap_sides {
            for cond in &mut conditions {
                cond.flip_sides();
            }
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
                    cardinality: StatisticsValue::Estimated(
                        node.subgraph.estimated_cardinality() as usize
                    ),
                },
                location: LocationRequirement::Any,
                children: vec![left, right],
            }))
        }
    }
}
