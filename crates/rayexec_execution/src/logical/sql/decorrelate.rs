use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::logical::{
    expr::{LogicalExpression, Subquery},
    operator::{Filter, LogicalOperator, Projection},
};
use rayexec_error::Result;

use super::scope::ColumnRef;

#[derive(Debug)]
pub struct SubqueryDecorrelator {}

impl SubqueryDecorrelator {
    pub fn plan_correlated(
        &mut self,
        subquery: &mut Subquery,
        input: &mut LogicalOperator,
        num_input_cols: usize,
    ) -> Result<LogicalExpression> {
        let mut root = *subquery.take_root();
        match subquery {
            Subquery::Scalar { .. } => {
                // TODO: Delim left (input)

                let mut dep_push_down = DependentJoinPushDown::new(num_input_cols);
                dep_push_down.find_correlated_columns(&mut root)?;
                dep_push_down.push_down(&mut root)?;

                unimplemented!()
            }
            Subquery::Exists { negated, .. } => {
                //
                unimplemented!()
            }
            Subquery::Any { .. } => unimplemented!(),
        }
    }
}

#[derive(Debug)]
struct DependentJoinPushDown {
    /// Correlated columns at each lateral level.
    ///
    /// (lateral_level -> column_ref)
    ///
    /// The column refs are btree since we want to maintain those in order.
    correlated: BTreeMap<usize, BTreeSet<ColumnRef>>,

    /// Computed offsets for new column references at each lateral level.
    ///
    /// Essentially this means for each lateral level, we'll be appending
    /// additional columns to the right by way of a dependent join. This lets us
    /// compute the new decorrelated column references quickly.
    lateral_offsets: BTreeMap<usize, usize>,

    /// Number of columns from the original input plan.
    ///
    /// This is used to compute the offset for the new column refs as we push
    /// down the dependent joins.
    num_input_cols: usize,
}

impl DependentJoinPushDown {
    fn new(num_input_cols: usize) -> Self {
        DependentJoinPushDown {
            correlated: BTreeMap::new(),
            lateral_offsets: BTreeMap::new(),
            num_input_cols,
        }
    }

    /// Finds all correlated columns in this plan, placing those columns in the
    /// `correlated` map.
    ///
    /// This is done prior to pushing down the dependent join so that we know
    /// how many joins we're working with, which let's us replace the correlated
    /// columns during the push down.
    fn find_correlated_columns(&mut self, root: &mut LogicalOperator) -> Result<()> {
        root.walk_mut_pre(&mut |plan| {
            match plan {
                LogicalOperator::Projection(Projection { exprs, .. }) => {
                    self.add_any_correlated_columns(exprs)?;
                }
                LogicalOperator::Filter(Filter { predicate, .. }) => {
                    self.add_any_correlated_columns([predicate])?;
                }
                _ => (), // TODO: More
            }
            Ok(())
        })?;

        // First lateral starts at the end of the original input.
        let mut curr_offset = self.num_input_cols;
        for (lateral, cols) in &self.correlated {
            self.lateral_offsets.insert(*lateral, curr_offset);
            // Subsequent laterals get added to the right.
            curr_offset += cols.len();
        }

        Ok(())
    }

    /// For an existing column ref, return the new column ref that would point
    /// to the resulting column in the join.
    fn decorrelated_ref(&self, col: ColumnRef) -> ColumnRef {
        // All unwraps indicate programmer bug. We should have already seen all
        // possible lateral levels and column references.

        let lateral_offset = self.lateral_offsets.get(&col.scope_level).unwrap();
        let lateral_cols = self.correlated.get(&col.scope_level).unwrap();

        let pos = lateral_cols
            .iter()
            .position(|correlated_col| correlated_col == &col)
            .unwrap();

        // New column ref is the offset of this lateral level + the offset of
        // the column _within_ this lateral level.
        ColumnRef {
            scope_level: 0,
            item_idx: lateral_offset + pos,
        }
    }

    /// Iterate and walk all the given expression, inserting correlated columns
    /// into the `correlated` map as they're encountered.
    fn add_any_correlated_columns<'a>(
        &mut self,
        exprs: impl IntoIterator<Item = &'a mut LogicalExpression>,
    ) -> Result<()> {
        use std::collections::btree_map::Entry;

        LogicalExpression::walk_mut_many(
            exprs,
            &mut |expr| match expr {
                LogicalExpression::ColumnRef(col) if col.scope_level > 0 => {
                    match self.correlated.entry(col.scope_level) {
                        Entry::Vacant(ent) => {
                            let mut cols = BTreeSet::new();
                            cols.insert(*col);
                            ent.insert(cols);
                        }
                        Entry::Occupied(mut ent) => {
                            ent.get_mut().insert(*col);
                        }
                    }
                    Ok(())
                }
                _ => Ok(()),
            },
            &mut |_| Ok(()),
        )
    }

    /// Rewrites correlated columns in the given expressions, returning the
    /// number of expressions that were rewritten.
    fn rewrite_correlated_columns<'a>(
        &self,
        exprs: impl IntoIterator<Item = &'a mut LogicalExpression>,
    ) -> Result<usize> {
        let mut num_rewritten = 0;
        LogicalExpression::walk_mut_many(
            exprs,
            &mut |expr| match expr {
                LogicalExpression::ColumnRef(col) if col.scope_level > 0 => {
                    *expr = LogicalExpression::ColumnRef(self.decorrelated_ref(*col));
                    num_rewritten += 1;
                    Ok(())
                }
                _ => Ok(()),
            },
            &mut |_| Ok(()),
        )?;
        Ok(num_rewritten)
    }

    /// Push down dependent joins.
    fn push_down(&self, root: &mut LogicalOperator) -> Result<()> {
        root.walk_mut_post(&mut |plan| match plan {
            LogicalOperator::Filter(Filter { predicate, .. }) => {
                // Filter is simple, don't need to do anything special.
                let _ = self.rewrite_correlated_columns([predicate])?;
                Ok(())
            }
            LogicalOperator::Projection(Projection { exprs, .. }) => {
                let num_rewritten = self.rewrite_correlated_columns(exprs)?;
                unimplemented!()
            }
            _ => unimplemented!(),
        })?;

        unimplemented!()
    }
}
