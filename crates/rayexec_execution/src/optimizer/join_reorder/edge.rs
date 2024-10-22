use std::collections::{HashMap, HashSet};
use std::fmt;

use rayexec_error::{RayexecError, Result};

use super::graph::{BaseRelation, GeneratedPlan, RelId};
use crate::explain::context_display::{debug_print_context, ContextDisplay, ContextDisplayMode};
use crate::expr::column_expr::ColumnExpr;
use crate::expr::Expression;
use crate::logical::binder::bind_context::TableRef;
use crate::logical::logical_join::ComparisonCondition;
use crate::optimizer::filter_pushdown::extracted_filter::ExtractedFilter;

pub type HyperEdgeId = usize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EdgeId {
    pub hyper_edge_id: HyperEdgeId,
    pub edge_id: usize,
}

/// All hyper edges for the graph.
#[derive(Debug)]
pub struct HyperEdges(pub Vec<HyperEdge>);

/// Hyper edge connecting two or more relations in the graph.
#[derive(Debug)]
pub struct HyperEdge {
    pub id: HyperEdgeId,
    /// All distinct edges making up this hyper edge.
    pub edges: HashMap<EdgeId, Edge>,
    /// Minimum num distinct values across all relations connected by this hyper
    /// edge.
    ///
    /// This is the basis for our cardinality estimate.
    pub min_ndv: f64,
    /// All column expressions within this hyper edge.
    pub columns: HashSet<ColumnExpr>,
}

/// Edge connecting extactly two relations in the graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Edge {
    /// The expression join 1 or 2 nodes.
    ///
    /// For join conditions, this will be joining 2 nodes. Simple filters will
    /// just be on one node (and only left refs will be populated).
    pub filter: Expression,
    /// Refs on the left side of the comparison.
    pub left_refs: HashSet<TableRef>,
    /// Refs on the right side of the comparison.
    pub right_refs: HashSet<TableRef>,
    /// Base relation the left side is pointing to.
    pub left_rel: HashSet<RelId>,
    /// Base relation the right side is pointing to.
    pub right_rel: HashSet<RelId>,
}

impl Edge {
    /// If this edge only references a single node in the graph.
    pub fn is_single_node(&self) -> bool {
        self.right_refs.is_empty()
    }
}

#[derive(Debug)]
pub struct FoundEdge<'a> {
    pub edge_id: EdgeId,
    pub edge: &'a Edge,
    pub min_ndv: f64,
}

impl HyperEdges {
    /// Create a new set of hyper edges from join conditions.
    ///
    /// Hyper edge NDV will be initialized from base relation cardinalities.
    pub fn new(
        conditions: impl IntoIterator<Item = ComparisonCondition>,
        filters: impl IntoIterator<Item = ExtractedFilter>,
        base_relations: &HashMap<RelId, BaseRelation>,
    ) -> Result<Self> {
        let mut hyper_edges = HyperEdges(Vec::new());

        for condition in conditions {
            hyper_edges.insert_condition_as_edge(condition, base_relations)?;
        }

        for filter in filters {
            hyper_edges.insert_expression_as_edge(filter, base_relations)?;
        }

        // TODO: Round of combining hyper edges.

        Ok(hyper_edges)
    }

    /// Find edges between two generated plans.
    pub fn find_edges(&self, p1: &GeneratedPlan, p2: &GeneratedPlan) -> Vec<FoundEdge> {
        let mut found = Vec::new();

        for hyper_edge in &self.0 {
            for (edge_id, edge) in &hyper_edge.edges {
                // Only consider conditions not previously used.
                if p1.used.edges.contains(edge_id) || p2.used.edges.contains(edge_id) {
                    continue;
                }

                // Edge between p1 and p2.
                if edge.left_refs.is_subset(&p1.output_refs)
                    && edge.right_refs.is_subset(&p2.output_refs)
                {
                    found.push(FoundEdge {
                        edge_id: *edge_id,
                        edge,
                        min_ndv: hyper_edge.min_ndv,
                    });
                }

                // Edge between p2 and p1 (reversed)
                //
                // Note we don't need to keep track if this is reversed, we'll
                // worry about that when we build up the plan.
                if edge.left_refs.is_subset(&p2.output_refs)
                    && edge.right_refs.is_subset(&p1.output_refs)
                {
                    found.push(FoundEdge {
                        edge_id: *edge_id,
                        edge,
                        min_ndv: hyper_edge.min_ndv,
                    });
                }

                // Not a valid edge, continue.
            }
        }

        found
    }

    pub fn remove_edge(&mut self, id: EdgeId) -> Option<Edge> {
        let hyper_edge = self.0.get_mut(id.hyper_edge_id)?;
        hyper_edge.edges.remove(&id)
    }

    /// Checks if all edges have been removed during the building of the final
    /// plan.
    pub fn all_edges_removed(&self) -> bool {
        for hyper_edge in &self.0 {
            if !hyper_edge.edges.is_empty() {
                return false;
            }
        }
        true
    }

    pub fn drain_edges(&mut self) -> impl Iterator<Item = Edge> + '_ {
        self.0
            .iter_mut()
            .flat_map(|hyp| hyp.edges.drain().map(|(_, edge)| edge))
    }

    fn insert_expression_as_edge(
        &mut self,
        filter: ExtractedFilter,
        base_relations: &HashMap<RelId, BaseRelation>,
    ) -> Result<()> {
        let mut min_ndv = f64::MAX;

        // For base filters, we're just going to track a single set of
        // refs/relations and place them in left.
        let mut left_rel = HashSet::new();

        for (&rel_id, rel) in base_relations {
            if filter.table_refs.is_subset(&rel.output_refs) {
                left_rel.insert(rel_id);
                // See comment in condition as edge for rationale.
                min_ndv = f64::min(min_ndv, rel.cardinality);
            }
        }

        let edge = Edge {
            filter: filter.filter,
            left_refs: filter.table_refs,
            right_refs: HashSet::new(),
            left_rel,
            right_rel: HashSet::new(),
        };

        let cols: HashSet<_> = filter.columns.into_iter().collect();

        for hyper_edge in &mut self.0 {
            if !hyper_edge.columns.is_disjoint(&cols) {
                // Hyper edge is connected. Add this edge to the hyper edge,
                // and update min_ndv if needed.
                let edge_id = EdgeId {
                    hyper_edge_id: hyper_edge.id,
                    edge_id: hyper_edge.edges.len(),
                };
                hyper_edge.edges.insert(edge_id, edge);

                // Add new columns.
                hyper_edge.columns.extend(cols);

                hyper_edge.min_ndv = f64::min(hyper_edge.min_ndv, min_ndv);

                // We're done, edge is now in the hyper graph.
                return Ok(());
            }
        }

        // No overlap with any existing edges. Initialize new one.
        let hyper_edge_id = self.0.len();
        let hyper_edge = HyperEdge {
            id: hyper_edge_id,
            edges: [(
                EdgeId {
                    hyper_edge_id,
                    edge_id: 0,
                },
                edge,
            )]
            .into_iter()
            .collect(),
            min_ndv,
            columns: cols,
        };

        self.0.push(hyper_edge);

        Ok(())
    }

    fn insert_condition_as_edge(
        &mut self,
        condition: ComparisonCondition,
        base_relations: &HashMap<RelId, BaseRelation>,
    ) -> Result<()> {
        let mut min_ndv = f64::MAX;

        let left_refs = condition.left.get_table_references();
        let right_refs = condition.right.get_table_references();

        let mut left_rel = HashSet::new();
        let mut right_rel = HashSet::new();

        for (&rel_id, rel) in base_relations {
            if left_refs.is_subset(&rel.output_refs) {
                left_rel.insert(rel_id);

                // Note we initialize NDV to relation cardinality which will
                // typically overestimate NDV, but by taking the min of all
                // cardinalities involved in the condition, we can
                // significantly reduce it.
                min_ndv = f64::min(min_ndv, rel.cardinality);
            }

            if right_refs.is_subset(&rel.output_refs) {
                right_rel.insert(rel_id);

                // See above.
                min_ndv = f64::min(min_ndv, rel.cardinality);
            }
        }

        // We have the "local" min_ndv, check existing hyper edges to see if
        // it can be added to one.

        let cols: HashSet<_> = condition
            .left
            .get_column_references()
            .into_iter()
            .chain(condition.right.get_column_references().into_iter())
            .collect();

        let edge = Edge {
            filter: condition.into_expression(),
            left_refs,
            right_refs,
            left_rel,
            right_rel,
        };

        for hyper_edge in &mut self.0 {
            if !hyper_edge.columns.is_disjoint(&cols) {
                // Hyper edge is connected. Add this edge to the hyper edge,
                // and update min_ndv if needed.
                let edge_id = EdgeId {
                    hyper_edge_id: hyper_edge.id,
                    edge_id: hyper_edge.edges.len(),
                };
                hyper_edge.edges.insert(edge_id, edge);

                // Add new columns.
                hyper_edge.columns.extend(cols);

                hyper_edge.min_ndv = f64::min(hyper_edge.min_ndv, min_ndv);

                // We're done, edge is now in the hyper graph.
                return Ok(());
            }
        }

        // No overlap with any existing edges. Initialize new one.
        let hyper_edge_id = self.0.len();
        let hyper_edge = HyperEdge {
            id: hyper_edge_id,
            edges: [(
                EdgeId {
                    hyper_edge_id,
                    edge_id: 0,
                },
                edge,
            )]
            .into_iter()
            .collect(),
            min_ndv,
            columns: cols,
        };

        self.0.push(hyper_edge);

        Ok(())
    }
}

impl ContextDisplay for HyperEdges {
    fn fmt_using_context(
        &self,
        mode: ContextDisplayMode,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        for hyp in &self.0 {
            writeln!(f, "Hyperedge: {}", hyp.id)?;
            writeln!(f, "  min_ndv: {}", hyp.min_ndv)?;
            writeln!(f, "  columns:")?;
            for col in &hyp.columns {
                write!(f, "    - ")?;
                col.fmt_using_context(mode, f)?;
                writeln!(f, "")?;
            }
        }
        Ok(())
    }
}
