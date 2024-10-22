use std::collections::{HashMap, HashSet};

use rayexec_error::{RayexecError, Result};

use super::edge::{FoundEdge, HyperEdge, HyperEdges};
use super::graph::{BaseRelation, GeneratedPlan, PlanKey, RelId};
use crate::expr::comparison_expr::ComparisonOperator;

/// Estimates the cardinality of joining two input plans.
#[derive(Debug)]
pub struct CardinalityEstimator<'a> {
    pub p1: &'a GeneratedPlan,
    pub p2: &'a GeneratedPlan,
    pub edges: &'a [FoundEdge<'a>],
    pub base_relations: &'a HashMap<RelId, BaseRelation>,
}

impl<'a> CardinalityEstimator<'a> {
    /// Estimates cardinality by taking the product of all input (base)
    /// relations and dividing by a selectivity denominator.
    pub fn estimated_cardinality(&mut self) -> Result<f64> {
        let subgraph = self.compute_sugraph()?;

        let mut numerator = 1.0;
        for rel_id in subgraph.relations {
            let rel = self.base_relations.get(&rel_id).unwrap();
            numerator *= rel.cardinality;
        }

        println!("D: {:>40} N: {:>40}", subgraph.selectivity_denom, numerator);

        Ok(numerator / subgraph.selectivity_denom)
    }

    fn compute_sugraph(&mut self) -> Result<Subgraph> {
        let mut subgraphs: Vec<Subgraph> = Vec::new();

        for edge in self.edges.iter() {
            let connected_indices = Subgraph::connected_indices(&subgraphs, &edge);

            match connected_indices.len() {
                0 => {
                    // Initial subgraph for this edge.
                    let mut subgraph = Subgraph {
                        relations: edge
                            .edge
                            .left_rel
                            .iter()
                            .chain(edge.edge.right_rel.iter())
                            .copied()
                            .collect(),
                        selectivity_denom: 1.0,
                    };

                    let dummy = Subgraph {
                        relations: HashSet::new(),
                        selectivity_denom: 1.0,
                    };

                    subgraph.update_denominator(&dummy, &edge, self.base_relations);

                    subgraphs.push(subgraph);
                }
                1 => {
                    //
                    unimplemented!()
                }
                2 => {
                    // Merge subgraphs connected by this edge.
                    let mut right = subgraphs.remove(connected_indices[1]);
                    let left = &mut subgraphs[connected_indices[0]];

                    left.relations.extend(right.relations.drain());
                    left.update_denominator(&right, &edge, self.base_relations);
                }
                other => {
                    return Err(RayexecError::new(format!(
                        "Unexpected number of connected indices: {other}"
                    )))
                }
            }
        }

        match subgraphs.len() {
            0 => {
                let unioned = self
                    .p1
                    .key
                    .0
                    .iter()
                    .chain(self.p2.key.0.iter())
                    .copied()
                    .collect();

                Ok(Subgraph {
                    relations: unioned,
                    selectivity_denom: 1.0,
                })
            }
            1 => Ok(subgraphs.pop().unwrap()),
            _ => {
                // Merge all subgraphs into single subgraph.
                let mut iter = subgraphs.into_iter();
                let mut merged = iter.next().unwrap();

                for mut subgraph in iter {
                    merged.relations.extend(subgraph.relations.drain());
                    merged.selectivity_denom *= subgraph.selectivity_denom;
                }

                Ok(merged)
            }
        }
    }
}

#[derive(Debug)]
struct Subgraph {
    /// Holds all relation ids that make up this subgraph.
    relations: HashSet<RelId>,
    /// The computed denominator thus far.
    selectivity_denom: f64,
}

impl Subgraph {
    fn is_connected(&self, edge: &FoundEdge) -> bool {
        edge.edge.left_rel.is_subset(&self.relations)
            || edge.edge.right_rel.is_subset(&self.relations)
    }

    /// Returns the indices of the the subgraphs connected by the given edge.
    fn connected_indices(subgraphs: &[Subgraph], edge: &FoundEdge) -> Vec<usize> {
        let mut indices = Vec::new();

        for (a_idx, subgraph_a) in subgraphs.iter().enumerate() {
            for (b_idx, subgraph_b) in subgraphs.iter().skip(a_idx + 1).enumerate() {
                if subgraph_a.is_connected(edge) && subgraph_b.is_connected(edge) {
                    indices.push(a_idx);
                    indices.push(b_idx);
                }
            }
        }

        indices
    }

    /// Updates this subgraph's selectivity denominator by an implied join from
    /// `other` subgraph.
    fn update_denominator(
        &mut self,
        other: &Subgraph,
        edge: &FoundEdge,
        base_relations: &HashMap<RelId, BaseRelation>,
    ) {
        let mut denom = self.selectivity_denom * other.selectivity_denom;

        match edge.edge.condition.op {
            ComparisonOperator::Eq => {
                // =
                denom *= edge.min_ndv
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

        self.selectivity_denom = denom;
    }
}
