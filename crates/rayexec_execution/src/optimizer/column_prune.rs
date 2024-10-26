use std::collections::{BTreeSet, HashMap, HashSet};

use rayexec_error::{RayexecError, Result};

use super::OptimizeRule;
use crate::expr::column_expr::ColumnExpr;
use crate::expr::Expression;
use crate::logical::binder::bind_context::{BindContext, MaterializationRef};
use crate::logical::logical_project::LogicalProject;
use crate::logical::operator::{LogicalNode, LogicalOperator, Node};

/// Prunes columns from the plan, potentially pushing down projects into scans.
///
/// Note that a previous iteration of this rule assumed that table refs were
/// unique within a plan. That is not the case, particularly with CTEs as they
/// get cloned into the plan during planning without altering any table refs.
/// TPCH query 15 triggered an error due to this, but I was not actually able to
/// easily write a minimal example that could reproduce it.
#[derive(Debug, Default)]
pub struct ColumnPrune {}

impl OptimizeRule for ColumnPrune {
    fn optimize(
        &mut self,
        bind_context: &mut BindContext,
        mut plan: LogicalOperator,
    ) -> Result<LogicalOperator> {
        let mut mat_refs = MaterializationReferences::default();
        let mut prune_state = PruneState {
            implicit_reference: true,
            current_references: HashSet::new(),
            updated_expressions: HashMap::new(),
        };
        prune_state.walk_plan(bind_context, &mut plan, &mut mat_refs)?;

        // Now prune columns inside materializations.
        for (mat_ref, col_refs) in mat_refs.references {
            if col_refs.all_implicitly_referenced {
                // Nothing we can do.
                continue;
            }

            let mat_plan = bind_context.get_materialization_mut(mat_ref)?.plan.take();
            // Initialize prune state with the collected referenced columns.
            let mut prune_state = PruneState {
                implicit_reference: col_refs.all_implicitly_referenced,
                current_references: col_refs.columns,
                updated_expressions: HashMap::new(),
            };
        }

        Ok(plan)
    }
}

/// Tracks references to materialized plan.
///
/// This is "global" to the plan as materializations can happen at any level.
#[derive(Debug, Default)]
struct MaterializationReferences {
    /// Maps a materialization ref to a set of columns for that materialization.
    references: HashMap<MaterializationRef, MaterializedColumnReferences>,
}

/// Tracks referenced columns within a single materialization.
#[derive(Debug, Default)]
struct MaterializedColumnReferences {
    /// If all columns are implicitly referenced.
    ///
    /// If true, we cannot do any column pruning.
    all_implicitly_referenced: bool,
    /// All columns encountered.
    columns: HashSet<ColumnExpr>,
}

impl MaterializationReferences {
    fn get_or_create(&mut self, mat: MaterializationRef) -> &mut MaterializedColumnReferences {
        if !self.references.contains_key(&mat) {
            self.references
                .insert(mat, MaterializedColumnReferences::default());
        }

        self.references.get_mut(&mat).unwrap()
    }
}

#[derive(Debug)]
struct PruneState {
    /// Whether or not all columns are implicitly referenced.
    ///
    /// If this is true, then we can't prune any columns.
    implicit_reference: bool,
    /// Column references encountered so far.
    ///
    /// This get's built up as we go down the plan tree.
    current_references: HashSet<ColumnExpr>,
    /// Mapping of old column refs to new expressions that should be used in
    /// place of the old columns.
    updated_expressions: HashMap<ColumnExpr, Expression>,
}

impl PruneState {
    fn new(implicit_reference: bool) -> Self {
        PruneState {
            implicit_reference,
            current_references: HashSet::new(),
            updated_expressions: HashMap::new(),
        }
    }

    /// Create a new prune state that's initialized with column expressions
    /// found in `parent.
    ///
    /// This should be used when walking through operators that don't expose
    /// table refs from child operators (e.g. project).
    fn new_from_parent_node(parent: &impl LogicalNode, implicit_reference: bool) -> Self {
        let mut current_references = HashSet::new();

        parent
            .for_each_expr(&mut |expr| {
                extract_column_exprs(expr, &mut current_references);
                Ok(())
            })
            .expect("extract to not fail");

        PruneState {
            implicit_reference,
            current_references,
            updated_expressions: HashMap::new(),
        }
    }

    /// Replaces and outdated column refs in the plan at this node.
    fn apply_updated_expressions(&self, plan: &mut impl LogicalNode) -> Result<()> {
        plan.for_each_expr_mut(&mut |expr| {
            replace_column_reference(expr, &self.updated_expressions);
            Ok(())
        })
    }

    /// Walk the plan.
    ///
    /// 1. Collect columns in use on the way down.
    /// 2. Reach a node we can't push through, replace projects as necessary.
    /// 3. Replace column references with updated references on the way up.
    ///
    /// When encountering a materialized scan, we just ensure all column
    /// references are placed into `mat_refs` as the column pruning for
    /// materializations will happen after we know the complete set of columns
    /// referenced.
    fn walk_plan(
        &mut self,
        bind_context: &mut BindContext,
        plan: &mut LogicalOperator,
        mat_refs: &mut MaterializationReferences,
    ) -> Result<()> {
        // Extract columns reference in this plan.
        //
        // Note that this may result in references tracked that we don't care
        // about, for example when at a 'project' node. We'll be creating a new
        // state when walking the child of a project, so these extra references
        // don't matter.
        //
        // The alternative could be to do this more selectively, but doesn't
        // seem worthwhile.
        plan.for_each_expr(&mut |expr| {
            extract_column_exprs(expr, &mut self.current_references);
            Ok(())
        })?;

        // Handle special node logic.
        //
        // This match determines which nodes can have projections pushed down
        // through, and which can't. The default case assumes we can't push down
        // projections.
        match plan {
            LogicalOperator::MaterializationScan(scan) => {
                let mat_cols = mat_refs.get_or_create(scan.node.mat);

                if self.implicit_reference {
                    // If outer has all columns implicitly referenced, then all
                    // columns in the materialization are also implicitly
                    // referenced.
                    mat_cols.all_implicitly_referenced = true;
                } else {
                    // Find all columns for the current set of referenced
                    // columns that are from the materialization, and add those.
                    let mat_table_refs: HashSet<_> = scan
                        .get_output_table_refs(bind_context)
                        .into_iter()
                        .collect();

                    for curr_col_ref in &self.current_references {
                        if mat_table_refs.contains(&curr_col_ref.table_scope) {
                            // Column is from materialization.
                            mat_cols.columns.insert(*curr_col_ref);
                        }
                    }
                }

                // Now we'll walk the materialized plan since itself may contain
                // references to other materialized plans.
                let mut mat_plan = bind_context
                    .get_materialization_mut(scan.node.mat)?
                    .plan
                    .take();
                // We create a new prune state with all top-level column
                // implicitly referenced. The prune state may prune some
                // columns, but we want to ensure we keep all top-level columns
                // as we don't yet have the complete set of references.
                // let new_prune_state = PruneState {
                //     implicit_reference: true,
                //     current_references: HashSet::new(),
                //     updated_expressions: HashMap::new(),
                // };
                // // new_prune_state.walk_plan(bind_context, &mut mat_plan, mat_refs)?;

                // // We don't need to update table refs or expressions since all
                // // output columns from the materialization remain unchanged.
                // let mat = bind_context.get_materialization_mut(scan.node.mat)?;
                // mat.plan = mat_plan;
            }
            LogicalOperator::MagicMaterializationScan(scan) => {
                // Similar to the normal materialization scan, just that we have
                // a projection out of it, so need to compare all column refs to
                // that projection, and not the table refs from the
                // materialization.
                //
                // Once we have all referenced projections, we then just get the
                // referenced materialized columns from those.
                let mat_cols = mat_refs.get_or_create(scan.node.mat);

                if self.implicit_reference {
                    mat_cols.all_implicitly_referenced = true;
                } else {
                    // Get only projections that are currently being referenced.
                    let mut referenced_projections = Vec::new();

                    for (col_idx, proj) in scan.node.projections.iter().enumerate() {
                        let check = ColumnExpr {
                            table_scope: scan.node.table_ref,
                            column: col_idx,
                        };

                        if self.current_references.contains(&check) {
                            referenced_projections.push(proj)
                        }
                    }

                    // Now extract the materialized columns from the referenced
                    // projections.
                    for referenced in referenced_projections {
                        extract_column_exprs(referenced, &mut mat_cols.columns);
                    }
                }
            }
            LogicalOperator::Project(project) => {
                // First try to flatten with child projection.
                try_flatten_projection(project)?;

                // Now check if we're actually referencing everything in the
                // projection.
                let proj_references: HashSet<_> = self
                    .current_references
                    .iter()
                    .filter(|col_expr| col_expr.table_scope == project.node.projection_table)
                    .copied()
                    .collect();

                // Special case for if this projection is just a pass through.
                if !self.implicit_reference && projection_is_passthrough(project, bind_context)? {
                    // New reference set we'll pass to child.
                    let mut child_references = HashSet::new();
                    let mut old_references = HashMap::new();

                    for (col_idx, projection) in project.node.projections.iter().enumerate() {
                        let old_column = ColumnExpr {
                            table_scope: project.node.projection_table,
                            column: col_idx,
                        };

                        if !proj_references.contains(&old_column) {
                            // Column not part of expression we're replacing nor
                            // expression we'll want to keep in the child.
                            continue;
                        }

                        let child_col = match projection {
                            Expression::Column(col) => *col,
                            other => {
                                return Err(RayexecError::new(format!(
                                    "Unexpected expression: {other}"
                                )))
                            }
                        };

                        child_references.insert(child_col);

                        // Map projection back to old column reference.
                        old_references.insert(child_col, old_column);
                    }

                    // Replace project plan with its child.
                    let mut child = project.take_one_child_exact()?;

                    let mut child_prune = PruneState {
                        implicit_reference: false,
                        current_references: child_references,
                        updated_expressions: HashMap::new(),
                    };
                    child_prune.walk_plan(bind_context, &mut child, mat_refs)?;

                    // Since we're removing the projection, no need to apply any
                    // changes here, but we'll need to propogate them up.
                    for (child_col, old_col) in old_references {
                        match child_prune.updated_expressions.get(&child_col) {
                            Some(updated) => {
                                // Map old column to updated child column.
                                self.updated_expressions.insert(old_col, updated.clone());
                            }
                            None => {
                                // Child didn't change, map old column to child
                                // column.
                                self.updated_expressions
                                    .insert(old_col, Expression::Column(child_col));
                            }
                        }
                    }

                    // Drop project, replace with child.
                    *plan = child;

                    // And we're done, project no longer part of plan.
                    return Ok(());
                }

                // Only create an updated projection if we're actually pruning
                // columns.
                //
                // If projection references is empty, then I'm not really sure.
                // Just skip for now.
                if !self.implicit_reference
                    && proj_references.len() != project.node.projections.len()
                    && !proj_references.is_empty()
                {
                    let mut new_proj_mapping: Vec<(ColumnExpr, Expression)> =
                        Vec::with_capacity(proj_references.len());

                    for (col_idx, projection) in project.node.projections.iter().enumerate() {
                        let old_column = ColumnExpr {
                            table_scope: project.node.projection_table,
                            column: col_idx,
                        };

                        if !proj_references.contains(&old_column) {
                            // Column not used, omit from the new projection
                            // we're building.
                            continue;
                        }

                        new_proj_mapping.push((old_column, projection.clone()));
                    }

                    // Generate the new table ref.
                    let table_ref =
                        bind_context.clone_to_new_ephemeral_table(project.node.projection_table)?;

                    // Generate the new projection, inserting updated
                    // expressions into the state.
                    let mut new_projections = Vec::with_capacity(new_proj_mapping.len());
                    for (col_idx, (old_column, projection)) in
                        new_proj_mapping.into_iter().enumerate()
                    {
                        new_projections.push(projection);

                        self.updated_expressions.insert(
                            old_column,
                            Expression::Column(ColumnExpr {
                                table_scope: table_ref,
                                column: col_idx,
                            }),
                        );
                    }

                    // Update this node.
                    project.node = LogicalProject {
                        projections: new_projections,
                        projection_table: table_ref,
                    };
                }

                // Now walk children using new prune state.
                let mut child_prune = PruneState::new_from_parent_node(project, false);
                for child in &mut project.children {
                    child_prune.walk_plan(bind_context, child, mat_refs)?;
                }
                child_prune.apply_updated_expressions(project)?;
            }
            LogicalOperator::Scan(scan) => {
                // BTree since we make the guarantee projections are ordered in
                // the scan.
                let mut cols: BTreeSet<_> = self
                    .current_references
                    .iter()
                    .filter_map(|col_expr| {
                        if col_expr.table_scope == scan.node.table_ref {
                            Some(col_expr.column)
                        } else {
                            None
                        }
                    })
                    .collect();

                // If we have an empty column list, then we're likely just
                // checking for the existence of rows. So just always include at
                // least one to make things easy for us.
                if cols.is_empty() {
                    cols.insert(
                        scan.node
                            .projection
                            .first()
                            .copied()
                            .ok_or_else(|| RayexecError::new("Scan references no columns"))?,
                    );
                }

                // Check if we're not referencing all columns. If so, we should
                // prune.
                let should_prune = scan.node.projection.iter().any(|col| !cols.contains(col));
                if !self.implicit_reference && should_prune {
                    // Prune by creating a new table with the pruned names and
                    // types. Create a mapping of original column -> new column.
                    let orig = bind_context.get_table(scan.node.table_ref)?;

                    // We manually pull out the original column name for the
                    // sake of a readable EXPLAIN instead of going with
                    // generated names.
                    let mut pruned_names = Vec::with_capacity(cols.len());
                    let mut pruned_types = Vec::with_capacity(cols.len());
                    for &col_idx in &cols {
                        pruned_names.push(orig.column_names[col_idx].clone());
                        pruned_types.push(orig.column_types[col_idx].clone());
                    }

                    let new_ref = bind_context.new_ephemeral_table_with_columns(
                        pruned_types.clone(),
                        pruned_names.clone(),
                    )?;

                    for (new_col, old_col) in cols.iter().copied().enumerate() {
                        self.updated_expressions.insert(
                            ColumnExpr {
                                table_scope: scan.node.table_ref,
                                column: old_col,
                            },
                            Expression::Column(ColumnExpr {
                                table_scope: new_ref,
                                column: new_col,
                            }),
                        );
                    }

                    // Update operator.
                    scan.node.did_prune_columns = true;
                    scan.node.types = pruned_types;
                    scan.node.names = pruned_names;
                    scan.node.table_ref = new_ref;
                    scan.node.projection = cols.into_iter().collect();
                }
            }
            LogicalOperator::Aggregate(agg) => {
                // Can't push down through aggregate, but we don't need to
                // assume everything is implicitly referenced for the children.
                let mut child_prune = PruneState::new_from_parent_node(agg, false);
                for child in &mut agg.children {
                    child_prune.walk_plan(bind_context, child, mat_refs)?;
                }
                child_prune.apply_updated_expressions(agg)?;
            }
            LogicalOperator::Filter(_) => {
                // Can push through filter.
                for child in plan.children_mut() {
                    self.walk_plan(bind_context, child, mat_refs)?;
                }
                self.apply_updated_expressions(plan)?;
            }
            LogicalOperator::Order(_) => {
                // Can push through order by.
                for child in plan.children_mut() {
                    self.walk_plan(bind_context, child, mat_refs)?;
                }
                self.apply_updated_expressions(plan)?;
            }
            LogicalOperator::Limit(_) => {
                // Can push through limit.
                for child in plan.children_mut() {
                    self.walk_plan(bind_context, child, mat_refs)?;
                }
                self.apply_updated_expressions(plan)?;
            }
            LogicalOperator::CrossJoin(_)
            | LogicalOperator::MagicJoin(_)
            | LogicalOperator::ComparisonJoin(_)
            | LogicalOperator::ArbitraryJoin(_) => {
                // All joins good to push through.
                for child in plan.children_mut() {
                    self.walk_plan(bind_context, child, mat_refs)?;
                }
                self.apply_updated_expressions(plan)?;
            }
            other => {
                // For all other plans, we take a conservative approach and not
                // push projections down through this node, but instead just
                // start working on the child plan.
                //
                // The child prune state is initialized from expressions at this
                // level.

                let mut child_prune = PruneState::new(true);
                other.for_each_expr(&mut |expr| {
                    extract_column_exprs(expr, &mut child_prune.current_references);
                    Ok(())
                })?;

                for child in other.children_mut() {
                    child_prune.walk_plan(bind_context, child, mat_refs)?;
                }

                // Note we apply from the child prune state since that's what's
                // actually holding the updated expressions that this node
                // should reference.
                child_prune.apply_updated_expressions(other)?;
            }
        }

        Ok(())
    }
}

/// Check if this project is just a simple pass through projection for its
/// child, and not actually needed.
///
/// A project is passthrough if it contains only column expressions with the
/// first expression starting at column 0 and every subsequent expression being
/// incremented by 1 up to num_cols
fn projection_is_passthrough(
    proj: &Node<LogicalProject>,
    bind_context: &BindContext,
) -> Result<bool> {
    let child_ref = match proj
        .get_one_child_exact()?
        .get_output_table_refs(bind_context)
        .first()
    {
        Some(table_ref) => *table_ref,
        None => return Ok(false),
    };

    for (check_idx, expr) in proj.node.projections.iter().enumerate() {
        let col = match expr {
            Expression::Column(col) => col,
            _ => return Ok(false),
        };

        if col.table_scope != child_ref {
            return Ok(false);
        }

        if col.column != check_idx {
            return Ok(false);
        }
    }

    Ok(true)
}

/// Recursively try to flatten this projection into a child projection.
///
/// If the projection's child is not a projection, nothing it done.
///
/// This does not change the table ref of this projection, and all column
/// references that reference this projection remain valid.
fn try_flatten_projection(current: &mut Node<LogicalProject>) -> Result<()> {
    assert_eq!(1, current.children.len());

    if !current.children[0].is_project() {
        // Not a project, nothing to do.
        return Ok(());
    }

    let mut child_projection = match current.take_one_child_exact()? {
        LogicalOperator::Project(project) => project,
        _ => unreachable!("operator has to be a project"),
    };

    // Try flattening child project first.
    try_flatten_projection(&mut child_projection)?;

    // Generate old -> new expression map from the child. We'll walk the parent
    // expression and just replace the old references.
    let expr_map: HashMap<ColumnExpr, Expression> = child_projection
        .node
        .projections
        .into_iter()
        .enumerate()
        .map(|(col_idx, expr)| {
            (
                ColumnExpr {
                    table_scope: child_projection.node.projection_table,
                    column: col_idx,
                },
                expr,
            )
        })
        .collect();

    current.for_each_expr_mut(&mut |expr| {
        replace_column_reference(expr, &expr_map);
        Ok(())
    })?;

    // Set this projection's children the child projection's children.
    current.children = child_projection.children;

    Ok(())
}

/// Replace all column references in the expression map with the associated
/// expression.
fn replace_column_reference(expr: &mut Expression, mapping: &HashMap<ColumnExpr, Expression>) {
    match expr {
        Expression::Column(col) => {
            if let Some(replace) = mapping.get(col) {
                *expr = replace.clone()
            }
        }
        other => other
            .for_each_child_mut(&mut |child| {
                replace_column_reference(child, mapping);
                Ok(())
            })
            .expect("replace to not fail"),
    }
}

fn extract_column_exprs(expr: &Expression, refs: &mut HashSet<ColumnExpr>) {
    match expr {
        Expression::Column(col) => {
            refs.insert(*col);
        }
        other => other
            .for_each_child(&mut |child| {
                extract_column_exprs(child, refs);
                Ok(())
            })
            .expect("extract not to fail"),
    }
}
