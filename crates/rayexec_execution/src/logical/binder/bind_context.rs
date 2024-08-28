use std::collections::{HashMap, HashSet};

use rayexec_bullet::datatype::DataType;
use rayexec_error::{RayexecError, Result};
use std::fmt;

/// Reference to a child bind scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BindScopeRef {
    pub context_idx: usize,
}

/// Reference to a table in a context.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableRef {
    pub table_idx: usize,
}

impl fmt::Display for TableRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.table_idx)
    }
}

#[derive(Debug)]
pub struct BindContext {
    /// All child scopes used for binding.
    ///
    /// Initialized with a single scope (root).
    scopes: Vec<BindScope>,

    /// All tables in the bind context. Tables may or may not be inside a scope.
    tables: Vec<Table>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CorrelatedColumn {
    /// Reference to an outer context the column is referencing.
    pub outer: BindScopeRef,
    pub table: TableRef,
    /// Index of the column in the table.
    pub col_idx: usize,
}

#[derive(Debug, Default)]
struct BindScope {
    /// Index to the parent bind context.
    ///
    /// Will be None if this is the root context.
    parent: Option<BindScopeRef>,

    /// Correlated columns in the query at this depth.
    correlated_columns: Vec<CorrelatedColumn>,

    /// Tables currently in scope.
    tables: Vec<TableRef>,
}

/// A "table" in the context.
///
/// These may have a direct relationship to an underlying base table, but may
/// also be used for ephemeral columns.
///
/// For example, when a query has aggregates in the select list, a separate
/// "aggregates" table will be created for hold columns that produce aggregates,
/// and the original select list will have their expressions replaced with
/// column references that point to this table.
#[derive(Debug)]
pub struct Table {
    pub reference: TableRef,
    pub alias: String,
    pub column_types: Vec<DataType>,
    pub column_names: Vec<String>,
}

impl Table {
    pub fn num_columns(&self) -> usize {
        self.column_types.len()
    }
}

impl BindContext {
    pub fn new() -> Self {
        BindContext {
            scopes: vec![BindScope {
                parent: None,
                tables: Vec::new(),
                correlated_columns: Vec::new(),
            }],
            tables: Vec::new(),
        }
    }

    pub fn root_scope_ref(&self) -> BindScopeRef {
        BindScopeRef { context_idx: 0 }
    }

    /// Creates a new bind scope, with current being the parent scope.
    ///
    /// The resulting scope should have visibility into parent scopes (for
    /// binding correlated columns).
    pub fn new_child_scope(&mut self, current: BindScopeRef) -> BindScopeRef {
        let idx = self.scopes.len();
        self.scopes.push(BindScope {
            parent: Some(current),
            tables: Vec::new(),
            correlated_columns: Vec::new(),
        });

        BindScopeRef { context_idx: idx }
    }

    /// Creates a new scope that has no parents, and thus no visibility into any
    /// other scope.
    pub fn new_orphan_scope(&mut self) -> BindScopeRef {
        let idx = self.scopes.len();
        self.scopes.push(BindScope {
            parent: None,
            tables: Vec::new(),
            correlated_columns: Vec::new(),
        });

        BindScopeRef { context_idx: idx }
    }

    pub fn get_parent_ref(&self, bind_ref: BindScopeRef) -> Result<Option<BindScopeRef>> {
        let child = self.get_scope(bind_ref)?;
        Ok(child.parent)
    }

    pub fn correlated_columns(&self, bind_ref: BindScopeRef) -> Result<&Vec<CorrelatedColumn>> {
        let child = self.get_scope(bind_ref)?;
        Ok(&child.correlated_columns)
    }

    /// Appends `other` context to `current`.
    ///
    /// Errors on duplicate table aliases.
    pub fn append_context(&mut self, current: BindScopeRef, other: BindScopeRef) -> Result<()> {
        let left_aliases: HashSet<_> = self.iter_tables(current)?.map(|t| &t.alias).collect();
        for table in self.iter_tables(other)? {
            if left_aliases.contains(&table.alias) {
                return Err(RayexecError::new(format!(
                    "Duplicate table name: {}",
                    table.alias
                )));
            }
        }

        // TODO: Correlated columns, USING
        let mut other_tables = {
            let other = self.get_scope(other)?;
            other.tables.clone()
        };

        let current = self.get_scope_mut(current)?;

        current.tables.append(&mut other_tables);

        Ok(())
    }

    /// Computes distance from child to parent, erroring if there's no
    /// connection between the refs.
    ///
    /// Counts "edges" between contexts, so the immediate parent of a child
    /// context will have a distance of 1.
    pub fn distance_child_to_parent(
        &self,
        child: BindScopeRef,
        parent: BindScopeRef,
    ) -> Result<usize> {
        let mut current = self.get_scope(child)?;
        let mut distance = 0;

        loop {
            distance += 1;
            let current_parent = match current.parent {
                Some(current_parent) => {
                    if parent == current_parent {
                        return Ok(distance);
                    }
                    current_parent
                }
                None => {
                    return Err(RayexecError::new(
                        "No connection between child and parent context",
                    ))
                }
            };

            current = self.get_scope(current_parent)?;
        }
    }

    /// Create a table that belong to no scope.
    pub fn new_ephemeral_table(&mut self) -> Result<TableRef> {
        self.new_ephemeral_table_with_columns(Vec::new(), Vec::new())
    }

    pub fn new_ephemeral_table_with_columns(
        &mut self,
        column_types: Vec<DataType>,
        column_names: Vec<String>,
    ) -> Result<TableRef> {
        let table_idx = self.tables.len();
        let reference = TableRef { table_idx };
        let scope = Table {
            reference,
            alias: String::new(),
            column_types,
            column_names,
        };
        self.tables.push(scope);

        Ok(reference)
    }

    pub fn push_column_for_table(
        &mut self,
        table: TableRef,
        name: impl Into<String>,
        datatype: DataType,
    ) -> Result<usize> {
        let table = self.get_table_mut(table)?;
        let idx = table.column_types.len();
        table.column_names.push(name.into());
        table.column_types.push(datatype);
        Ok(idx)
    }

    pub fn get_table_mut(&mut self, table_ref: TableRef) -> Result<&mut Table> {
        self.tables
            .get_mut(table_ref.table_idx)
            .ok_or_else(|| RayexecError::new("Missing table scope in bind context"))
    }

    pub fn push_table(
        &mut self,
        idx: BindScopeRef,
        alias: impl Into<String>,
        column_types: Vec<DataType>,
        column_names: Vec<String>,
    ) -> Result<TableRef> {
        let alias = alias.into();

        for scope in self.iter_tables(idx)? {
            if scope.alias == alias {
                return Err(RayexecError::new(format!("Duplicate table name: {alias}")));
            }
        }

        let table_idx = self.tables.len();
        let reference = TableRef { table_idx };
        let scope = Table {
            reference,
            alias,
            column_types,
            column_names,
        };
        self.tables.push(scope);

        let child = self.get_scope_mut(idx)?;
        child.tables.push(reference);

        Ok(reference)
    }

    pub fn append_table_to_scope(&mut self, scope: BindScopeRef, table: TableRef) -> Result<()> {
        // TODO: Probably check columns for duplicates.
        let scope = self.get_scope_mut(scope)?;
        scope.tables.push(table);
        Ok(())
    }

    pub fn push_correlation(
        &mut self,
        idx: BindScopeRef,
        correlation: CorrelatedColumn,
    ) -> Result<()> {
        let child = self.get_scope_mut(idx)?;
        child.correlated_columns.push(correlation);
        Ok(())
    }

    /// Tries to find the the scope that has a matching column name.
    ///
    /// This will only search the current scope, and will not look at any outer
    /// scopes.
    ///
    /// Returns the table, and the relative index of the column within that table.
    pub fn find_table_for_column(
        &self,
        current: BindScopeRef,
        column: &str,
    ) -> Result<Option<(&Table, usize)>> {
        let mut found = None;
        for table in self.iter_tables(current)? {
            for (col_idx, col_name) in table.column_names.iter().enumerate() {
                if col_name == column {
                    if found.is_some() {
                        return Err(RayexecError::new(format!("Ambiguous column name {column}")));
                    }
                    found = Some((table, col_idx));
                }
            }
        }

        Ok(found)
    }

    pub fn get_table(&self, scope_ref: TableRef) -> Result<&Table> {
        self.tables
            .get(scope_ref.table_idx)
            .ok_or_else(|| RayexecError::new("Missing table scope"))
    }

    /// Iterate tables in the given bind scope.
    pub fn iter_tables(&self, current: BindScopeRef) -> Result<impl Iterator<Item = &Table>> {
        let context = self.get_scope(current)?;
        Ok(context
            .tables
            .iter()
            .map(|table| &self.tables[table.table_idx]))
    }

    fn get_scope(&self, bind_ref: BindScopeRef) -> Result<&BindScope> {
        self.scopes
            .get(bind_ref.context_idx)
            .ok_or_else(|| RayexecError::new("Missing child bind context"))
    }

    fn get_scope_mut(&mut self, bind_ref: BindScopeRef) -> Result<&mut BindScope> {
        self.scopes
            .get_mut(bind_ref.context_idx)
            .ok_or_else(|| RayexecError::new("Missing child bind context"))
    }
}

#[cfg(test)]
pub(crate) mod testutil {
    //! Test utilities for the bind context.

    use super::*;

    /// Collect all (name, type) pairs for columns in the current scope.
    pub fn columns_in_scope(
        bind_context: &BindContext,
        scope: BindScopeRef,
    ) -> Vec<(String, DataType)> {
        bind_context
            .iter_tables(scope)
            .unwrap()
            .flat_map(|t| {
                t.column_names
                    .iter()
                    .cloned()
                    .zip(t.column_types.iter().cloned())
                    .into_iter()
            })
            .collect()
    }
}