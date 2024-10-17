use std::cmp::Ordering;
use std::collections::{HashSet, VecDeque};

use rayexec_error::{RayexecError, Result};

use super::filter_pushdown::extracted_filter::ExtractedFilter;
use super::filter_pushdown::split::split_conjunction;
use super::OptimizeRule;
use crate::expr::{self, Expression};
use crate::logical::binder::bind_context::{BindContext, TableRef};
use crate::logical::logical_filter::LogicalFilter;
use crate::logical::logical_join::{JoinType, LogicalComparisonJoin, LogicalCrossJoin};
use crate::logical::operator::{LocationRequirement, LogicalNode, LogicalOperator, Node};
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
    filters: Vec<ExtractedFilter>,
    child_plans: Vec<LogicalOperator>,
}

impl InnerJoinReorder {
    // TODO: Duplicated with filter pushdown.
    fn add_filter(&mut self, expr: Expression) {
        let mut split = Vec::new();
        split_conjunction(expr, &mut split);

        self.filters
            .extend(split.into_iter().map(ExtractedFilter::from_expr))
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

        let mut join_tree = JoinTree::new(child_plans, self.filters.drain(..));
        // Do the magic.
        let plan = join_tree.try_build()?;

        Ok(plan)
    }

    fn extract_filters_and_join_children(&mut self, root: LogicalOperator) -> Result<()> {
        assert!(self.filters.is_empty());
        assert!(self.child_plans.is_empty());

        let mut queue: VecDeque<_> = [root].into_iter().collect();

        while let Some(plan) = queue.pop_front() {
            match plan {
                LogicalOperator::Filter(mut filter) => {
                    self.add_filter(filter.node.filter);
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
                            self.add_filter(condition.into_expression());
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
                        self.add_filter(join.node.condition);
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

/// A substree of the query.
#[derive(Debug)]
struct JoinTree {
    nodes: Vec<JoinTreeNode>,
    filters: Vec<ExtractedFilter>,
    constant_filters: Vec<Expression>,
}

/// Represents a single node in the join subtree.
#[derive(Debug, Default)]
struct JoinTreeNode {
    /// If this node is valid.
    ///
    /// As we combine the nodes, into a tree with a filters in the right spot,
    /// some nodes will becoming invalid.
    valid: bool,
    /// All output refs for this node.
    output_refs: HashSet<TableRef>,
    /// The plan making up this node.
    ///
    /// If this is valid, this should never be None.
    plan: Option<LogicalOperator>,
    /// All filters that we know apply to this node in the tree.
    filters: Vec<Expression>,
}

impl JoinTree {
    fn new(
        plans: impl IntoIterator<Item = LogicalOperator>,
        filters: impl IntoIterator<Item = ExtractedFilter>,
    ) -> Self {
        // Initialize all nodes with empty filters and single child.
        let nodes: Vec<_> = plans
            .into_iter()
            .map(|p| JoinTreeNode {
                valid: true,
                output_refs: p.get_output_table_refs().into_iter().collect(),
                plan: Some(p),
                filters: Vec::new(),
            })
            .collect();

        // Collect all filters, then sort.
        let mut filters: Vec<ExtractedFilter> = filters.into_iter().collect();
        filters.sort_unstable_by(|a, b| filter_sort_compare(a, b).reverse()); // Reversed since we're going to treat the vec as a stack.

        JoinTree {
            nodes,
            filters,
            constant_filters: Vec::new(),
        }
    }

    fn try_build(&mut self) -> Result<LogicalOperator> {
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

        assert!(self.filters.is_empty());

        let mut built_nodes = self
            .nodes
            .drain(..)
            .filter_map(|n| if n.valid { Some(n) } else { None });

        let node = match built_nodes.next() {
            Some(plan) => plan,
            None => return Err(RayexecError::new("Join tree has no built nodes")),
        };

        let mut plan = match expr::and(node.filters) {
            Some(filter) => LogicalOperator::Filter(Node {
                node: LogicalFilter { filter },
                location: LocationRequirement::Any,
                children: vec![node.plan.expect("plan to be some")],
            }),
            None => node.plan.expect("plan to be some"),
        };

        // Cross join with other remaining nodes if needed.
        for right in built_nodes {
            let right = match expr::and(right.filters) {
                Some(filter) => LogicalOperator::Filter(Node {
                    node: LogicalFilter { filter },
                    location: LocationRequirement::Any,
                    children: vec![right.plan.expect("plan to be some")],
                }),
                None => right.plan.expect("plan to be some"),
            };

            plan = LogicalOperator::CrossJoin(Node {
                node: LogicalCrossJoin,
                location: LocationRequirement::Any,
                children: vec![plan, right],
            });
        }

        // Apply constant filters if needed.
        if let Some(filter) = expr::and(self.constant_filters.drain(..)) {
            plan = LogicalOperator::Filter(Node {
                node: LogicalFilter { filter },
                location: LocationRequirement::Any,
                children: vec![plan],
            });
        }

        Ok(plan)
    }

    fn try_combine_step(&mut self) -> Result<bool> {
        let filter = match self.filters.pop() {
            Some(filter) => filter,
            None => return Ok(false),
        };

        // Figure out which nodes this filter can possibly apply to.
        let node_indices: Vec<_> = self
            .nodes
            .iter()
            .enumerate()
            .filter_map(|(idx, node)| {
                if !node.valid {
                    return None;
                }

                // Only include nodes that this filter has a reference for.
                if filter.tables_refs.is_disjoint(&node.output_refs) {
                    return None;
                }

                Some(idx)
            })
            .collect();

        match node_indices.len() {
            0 => {
                if !filter.tables_refs.is_empty() {
                    // Shouldn't happen.
                    return Err(RayexecError::new(format!(
                        "Filter does not apply to any nodes in the subtree: {filter:?}"
                    )));
                }

                // Filter does not depend on any plans.
                self.constant_filters.push(filter.filter);
            }
            1 => {
                // Filter applies to just one node in the tree, not a join
                // condition.
                let idx = node_indices[0];
                self.nodes[idx].filters.push(filter.filter);
            }
            2 => {
                // We're referencing two nodes in the tree. Now we combine them
                // into an inner join.
                //
                // These takes will mark the nodes as invalid for the next call
                // (via the default impl)
                let left = std::mem::take(&mut self.nodes[node_indices[0]]);
                let right = std::mem::take(&mut self.nodes[node_indices[1]]);

                // Swap if needed.
                let [mut left, mut right] = Self::maybe_swap_using_stats([left, right]);

                let left_refs: Vec<_> = left.output_refs.iter().copied().collect();
                let right_refs: Vec<_> = right.output_refs.iter().copied().collect();

                let extractor =
                    JoinConditionExtractor::new(&left_refs, &right_refs, JoinType::Inner);
                let mut conditions = extractor.extract(vec![filter.filter])?;

                // Extend node specific filters.
                left.filters.append(&mut conditions.left_filter);
                right.filters.append(&mut conditions.right_filter);

                // Build up left side of join.
                let mut left_plan = left.plan.take().expect("plan to be some");
                if let Some(filter) = expr::and(left.filters) {
                    left_plan = LogicalOperator::Filter(Node {
                        node: LogicalFilter { filter },
                        location: LocationRequirement::Any,
                        children: vec![left_plan],
                    });
                }

                // Build up right side;
                let mut right_plan = right.plan.take().expect("plan to be some");
                if let Some(filter) = expr::and(right.filters) {
                    right_plan = LogicalOperator::Filter(Node {
                        node: LogicalFilter { filter },
                        location: LocationRequirement::Any,
                        children: vec![right_plan],
                    });
                }

                // Now do the join.
                let join = LogicalOperator::ComparisonJoin(Node {
                    node: LogicalComparisonJoin {
                        join_type: JoinType::Inner,
                        conditions: conditions.comparisons,
                    },
                    location: LocationRequirement::Any,
                    children: vec![left_plan, right_plan],
                });

                // Push back new node.
                //
                // Next iteration will now have this node available to pick
                // from.
                self.nodes.push(JoinTreeNode {
                    valid: true,
                    output_refs: join.get_output_table_refs().into_iter().collect(),
                    plan: Some(join),
                    filters: conditions.arbitrary,
                });
            }
            _ => {
                // > 2 nodes.
                //
                // Arbitrarily cross join two of them, and push back the filter
                // to try again.
                let left = std::mem::take(&mut self.nodes[node_indices[0]]);
                let right = std::mem::take(&mut self.nodes[node_indices[1]]);

                // Swap if needed.
                let [mut left, mut right] = Self::maybe_swap_using_stats([left, right]);

                // Build up left side of join.
                let mut left_plan = left.plan.take().expect("plan to be some");
                if let Some(filter) = expr::and(left.filters) {
                    left_plan = LogicalOperator::Filter(Node {
                        node: LogicalFilter { filter },
                        location: LocationRequirement::Any,
                        children: vec![left_plan],
                    });
                }

                // Build up right side;
                let mut right_plan = right.plan.take().expect("plan to be some");
                if let Some(filter) = expr::and(right.filters) {
                    right_plan = LogicalOperator::Filter(Node {
                        node: LogicalFilter { filter },
                        location: LocationRequirement::Any,
                        children: vec![right_plan],
                    });
                }

                let join = LogicalOperator::CrossJoin(Node {
                    node: LogicalCrossJoin,
                    location: LocationRequirement::Any,
                    children: vec![left_plan, right_plan],
                });

                self.nodes.push(JoinTreeNode {
                    valid: true,
                    output_refs: join.get_output_table_refs().into_iter().collect(),
                    plan: Some(join),
                    filters: Vec::new(),
                });

                // Note we push the filter back so that we try again with the
                // same filter on the next iteration.
                //
                // We don't put it in the join tree node since it's still a
                // candidate to be a join condition, just with one of the
                // children being a cross join.
                self.filters.push(filter)
            }
        }

        Ok(true)
    }

    fn maybe_swap_using_stats([left, right]: [JoinTreeNode; 2]) -> [JoinTreeNode; 2] {
        let left_stats = left
            .plan
            .as_ref()
            .expect("left plan to exist")
            .get_statistics();
        let right_stats = right
            .plan
            .as_ref()
            .expect("right plan to exist")
            .get_statistics();

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

/// Sort function we're using for ordering the filters by which filter we should
/// try applying first.
///
/// This will sort filters that are equality join candidates first, followed by
/// filters sorted by number of table refs they reference (fewer table refs
/// roughly indicate they can be pushed further down into the tree).
fn filter_sort_compare(a: &ExtractedFilter, b: &ExtractedFilter) -> Ordering {
    // Try to sort with possible equalities coming first.
    let a_possible_equality = a.is_equality_join_candidate();
    let b_possible_equality = b.is_equality_join_candidate();

    if a_possible_equality && b_possible_equality {
        return Ordering::Equal;
    }

    if a_possible_equality {
        return Ordering::Less;
    }

    if b_possible_equality {
        return Ordering::Greater;
    }

    // Otherwise sort by which expression has fewer table refs.
    a.tables_refs.len().cmp(&b.tables_refs.len())
}
