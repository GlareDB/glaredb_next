use std::collections::{BTreeSet, HashMap, HashSet};

use rayexec_error::Result;
use tracing::warn;

use super::OptimizeRule;
use crate::expr::column_expr::ColumnExpr;
use crate::expr::Expression;
use crate::logical::binder::bind_context::{BindContext, MaterializationRef, TableRef};
use crate::logical::logical_project::LogicalProject;
use crate::logical::operator::{LogicalNode, LogicalOperator, Node};

#[derive(Debug, Default)]
pub struct ColumnPrune {
    /// Materializations we've found. Holds a boolean so we don't try to
    /// recursively collect columns (shouldn't be possible yet).
    materializations: HashMap<MaterializationRef, bool>,
    /// All column exprs we've found in the query.
    column_exprs: HashMap<TableRef, BTreeSet<usize>>,
    /// Remap of old column expression to new column expression.
    column_remap: HashMap<ColumnExpr, ColumnExpr>,
}

impl OptimizeRule for ColumnPrune {
    fn optimize(
        &mut self,
        bind_context: &mut BindContext,
        mut plan: LogicalOperator,
    ) -> Result<LogicalOperator> {
        // Find all columns we're referencing in the query
        self.collect_columns(&plan)?;

        // Update materializations hashmap so we don't recurse.
        for (_, handled) in self.materializations.iter_mut() {
            *handled = true;
        }

        // Handle all referenced materializations.
        let refs: Vec<_> = self.materializations.keys().copied().collect();
        for &mat_ref in &refs {
            let materialization = bind_context.get_materialization(mat_ref)?;
            self.collect_columns(&materialization.plan)?;
        }

        // Update scans.
        self.update_scans(bind_context, &mut plan)?;
        for &mat_ref in &refs {
            let mut orig = {
                let materialization = bind_context.get_materialization_mut(mat_ref)?;
                std::mem::replace(&mut materialization.plan, LogicalOperator::Invalid)
            };
            self.update_scans(bind_context, &mut orig)?;

            let materialization = bind_context.get_materialization_mut(mat_ref)?;
            materialization.plan = orig;
        }

        // Replace references.
        self.replace_references(bind_context, &mut plan)?;
        for &mat_ref in &refs {
            let mut orig = {
                let materialization = bind_context.get_materialization_mut(mat_ref)?;
                std::mem::replace(&mut materialization.plan, LogicalOperator::Invalid)
            };
            self.replace_references(bind_context, &mut orig)?;

            let materialization = bind_context.get_materialization_mut(mat_ref)?;
            materialization.plan = orig;
        }

        Ok(plan)
    }
}

impl ColumnPrune {
    fn collect_columns(&mut self, plan: &LogicalOperator) -> Result<()> {
        match plan {
            LogicalOperator::MaterializationScan(scan) => {
                if *self.materializations.get(&scan.node.mat).unwrap_or(&false) {
                    return Ok(());
                }
                self.materializations.insert(scan.node.mat, false);
            }
            other => {
                other.for_each_expr(&mut |expr| {
                    extract_from_expr(expr, &mut self.column_exprs);
                    Ok(())
                })?;

                for child in plan.children() {
                    self.collect_columns(child)?;
                }
            }
        }

        Ok(())
    }

    fn replace_references(
        &mut self,
        bind_context: &mut BindContext,
        plan: &mut LogicalOperator,
    ) -> Result<()> {
        if let LogicalOperator::MaterializationScan(scan) = plan {
            // TODO: Lotsa caching with the table refs for these.
            let materialization = bind_context.get_materialization_mut(scan.node.mat)?;
            let table_refs = materialization.plan.get_output_table_refs();
            materialization.table_refs = table_refs.clone();
            scan.node.table_refs = table_refs;
        }

        plan.for_each_expr_mut(&mut |expr| {
            replace_column_reference2(expr, &self.column_remap);
            Ok(())
        })?;

        for child in plan.children_mut() {
            self.replace_references(bind_context, child)?;
        }

        Ok(())
    }

    fn update_scans(
        &mut self,
        bind_context: &mut BindContext,
        plan: &mut LogicalOperator,
    ) -> Result<()> {
        match plan {
            LogicalOperator::Scan(scan) => {
                let cols = match self.column_exprs.remove(&scan.node.table_ref) {
                    Some(cols) => cols,
                    None => {
                        // TODO: I think we could just remove the table?
                        warn!(?scan, "Nothing referencing table");
                        return Ok(());
                    }
                };

                // Check if we're not referencing all columns. If so, we should
                // prune.
                let should_prune = scan.node.projection.iter().any(|col| !cols.contains(col));
                if !should_prune {
                    return Ok(());
                }

                // Prune by creating a new table with the pruned names and
                // types. Create a mapping of original column -> new column.

                let orig = bind_context.get_table(scan.node.table_ref)?;

                let mut pruned_names = Vec::with_capacity(cols.len());
                let mut pruned_types = Vec::with_capacity(cols.len());
                for &col_idx in &cols {
                    pruned_names.push(orig.column_names[col_idx].clone());
                    pruned_types.push(orig.column_types[col_idx].clone());
                }

                let new_ref = bind_context
                    .new_ephemeral_table_with_columns(pruned_types.clone(), pruned_names.clone())?;

                for (new_col, old_col) in cols.iter().copied().enumerate() {
                    self.column_remap.insert(
                        ColumnExpr {
                            table_scope: scan.node.table_ref,
                            column: old_col,
                        },
                        ColumnExpr {
                            table_scope: new_ref,
                            column: new_col,
                        },
                    );
                }

                // Update operator.
                scan.node.did_prune_columns = true;
                scan.node.types = pruned_types;
                scan.node.names = pruned_names;
                scan.node.table_ref = new_ref;
                scan.node.projection = cols.into_iter().collect();

                Ok(())
            }
            other => {
                for child in other.children_mut() {
                    self.update_scans(bind_context, child)?;
                }

                Ok(())
            }
        }
    }
}

#[derive(Debug, Default)]
struct PruneState {
    /// Column references encountered so far.
    ///
    /// This get's built up as we go down the plan tree.
    current_references: HashSet<ColumnExpr>,
    /// Mapping of old column refs to new expressions that should be used in
    /// place of the old columns.
    updated_expressions: HashMap<ColumnExpr, Expression>,
}

impl PruneState {
    /// Create a new prune state that's initialized with column expressions
    /// found in `parent.
    ///
    /// This should be used when walking through operators that don't expose
    /// table refs from child operators (e.g. project).
    fn new_from_parent(parent: &impl LogicalNode) -> Self {
        let mut current_references = HashSet::new();

        parent
            .for_each_expr(&mut |expr| {
                extract_column_exprs(expr, &mut current_references);
                Ok(())
            })
            .expect("extract to not fail");

        PruneState {
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
    fn walk_plan(
        &mut self,
        bind_context: &mut BindContext,
        plan: &mut LogicalOperator,
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

                // Only create an updated projection if we're actually pruning
                // columns.
                //
                // If projection references is empty, then we're either at the
                // root, or something else (idk yet).
                if proj_references.len() != project.node.projections.len()
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
                    let table_ref = bind_context.new_ephemeral_table_from_expressions(
                        "__generated_pruned_projection",
                        new_proj_mapping.iter().map(|(_, expr)| expr),
                    )?;

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

                    // Now walk children using new prune state.
                    let mut child_prune = PruneState::new_from_parent(project);
                    for child in &mut project.children {
                        child_prune.walk_plan(bind_context, child)?;
                    }

                    self.apply_updated_expressions(project)?;
                }
            }
            other => {
                // For all other plans, we take a conservative approach and not
                // push projections down through this node, but instead just
                // start working on the child plan.
                //
                // The child prune state is initialized from expressions at this
                // level.

                let mut child_prune = PruneState::default();
                other.for_each_expr(&mut |expr| {
                    extract_column_exprs(expr, &mut child_prune.current_references);
                    Ok(())
                })?;

                for child in other.children_mut() {
                    child_prune.walk_plan(bind_context, child)?;
                }

                self.apply_updated_expressions(other)?;
            }
        }

        Ok(())
    }
}

/// Tries to flatten this projection into a child projection.
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

    let child_projection = match current.take_one_child_exact()? {
        LogicalOperator::Project(project) => project,
        _ => unreachable!("operator has to a project"),
    };

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

fn replace_column_reference2(expr: &mut Expression, mapping: &HashMap<ColumnExpr, ColumnExpr>) {
    match expr {
        Expression::Column(col) => {
            if let Some(replace) = mapping.get(col).copied() {
                *col = replace;
            }
        }
        other => other
            .for_each_child_mut(&mut |child| {
                replace_column_reference2(child, mapping);
                Ok(())
            })
            .expect("replace to not fail"),
    }
}

fn extract_from_expr(expr: &Expression, extracted: &mut HashMap<TableRef, BTreeSet<usize>>) {
    match expr {
        Expression::Column(col) => {
            extracted
                .entry(col.table_scope)
                .and_modify(|cols| {
                    cols.insert(col.column);
                })
                .or_insert([col.column].into());
        }
        other => other
            .for_each_child(&mut |child| {
                extract_from_expr(child, extracted);
                Ok(())
            })
            .expect("extract to not fail"),
    }
}
