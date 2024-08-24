use std::collections::{HashMap, HashSet};

use rayexec_bullet::datatype::DataType;
use rayexec_error::{RayexecError, Result};

/// Reference to a child bind context.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BindContextRef {
    pub context_idx: usize,
}

/// Reference to a table scope in a context.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableScopeRef {
    pub table_idx: usize,
}

#[derive(Debug)]
pub struct BindContext {
    /// All child contexts used for binding.
    ///
    /// Initialized with a single child context (root).
    contexts: Vec<ChildBindContext>,

    /// All tables across all child contexts.
    tables: Vec<Table>,
}

#[derive(Debug, Clone)]
pub struct CorrelatedColumn {
    /// Reference to an outer context the column is referencing.
    pub outer: BindContextRef,
    pub table: TableScopeRef,
    /// Index of the column in the table.
    pub col_idx: usize,
}

#[derive(Debug, Default)]
struct ChildBindContext {
    /// Index to the parent bind context.
    ///
    /// Will be None if this is the root context.
    parent: Option<BindContextRef>,

    /// Correlated columns in the query at this depth.
    correlated_columns: Vec<CorrelatedColumn>,

    /// Scopes currently at this depth.
    scopes: Vec<TableScopeRef>,
}

#[derive(Debug)]
pub struct Table {
    pub reference: TableScopeRef,
    pub alias: String,
    pub column_types: Vec<DataType>,
    pub column_names: Vec<String>,
}

impl BindContext {
    pub fn new() -> Self {
        BindContext {
            contexts: vec![ChildBindContext {
                parent: None,
                scopes: Vec::new(),
                correlated_columns: Vec::new(),
            }],
            tables: Vec::new(),
        }
    }

    pub fn new_child(&mut self, current: BindContextRef) -> BindContextRef {
        let idx = self.contexts.len();
        self.contexts.push(ChildBindContext {
            parent: Some(current),
            scopes: Vec::new(),
            correlated_columns: Vec::new(),
        });

        BindContextRef { context_idx: idx }
    }

    pub fn get_parent_ref(&self, bind_ref: BindContextRef) -> Result<Option<BindContextRef>> {
        let child = self.get_child_context(bind_ref)?;
        Ok(child.parent)
    }

    pub fn correlated_columns(&self, bind_ref: BindContextRef) -> Result<&Vec<CorrelatedColumn>> {
        let child = self.get_child_context(bind_ref)?;
        Ok(&child.correlated_columns)
    }

    /// Appends `other` context to `current`.
    ///
    /// Errors on duplicate table aliases.
    pub fn append_context(&mut self, current: BindContextRef, other: BindContextRef) -> Result<()> {
        let left_aliases: HashSet<_> = self.iter_table_scopes(current)?.map(|t| &t.alias).collect();
        for table in self.iter_table_scopes(other)? {
            if left_aliases.contains(&table.alias) {
                return Err(RayexecError::new(format!(
                    "Duplicate table name: {}",
                    table.alias
                )));
            }
        }

        // TODO: Correlated columns, USING
        let mut other_tables = {
            let other = self.get_child_context(other)?;
            other.scopes.clone()
        };

        let current = self.get_child_context_mut(current)?;

        current.scopes.append(&mut other_tables);

        Ok(())
    }

    /// Computes distance from child to parent, erroring if there's no
    /// connection between the refs.
    ///
    /// Counts "edges" between contexts, so the immediate parent of a child
    /// context will have a distance of 1.
    pub fn distance_child_to_parent(
        &self,
        child: BindContextRef,
        parent: BindContextRef,
    ) -> Result<usize> {
        let mut current = self.get_child_context(child)?;
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

            current = self.get_child_context(current_parent)?;
        }
    }

    /// Pushes an empty table scope tot he current context.
    ///
    /// This allows us to generate a table scope reference for the select list
    /// prior to having all expressions planned. This lets us stub the table
    /// scope to allow for things that reference the select list to do so using
    /// normal column expressions (e.g. ORDER BY).
    pub fn push_empty_scope(&mut self, bind_ref: BindContextRef) -> Result<TableScopeRef> {
        self.push_table_scope(bind_ref, "empty", Vec::new(), Vec::new())
    }

    pub fn push_table_scope(
        &mut self,
        idx: BindContextRef,
        alias: impl Into<String>,
        column_types: Vec<DataType>,
        column_names: Vec<String>,
    ) -> Result<TableScopeRef> {
        let alias = alias.into();

        for scope in self.iter_table_scopes(idx)? {
            if scope.alias == alias {
                return Err(RayexecError::new(format!("Duplicate table name: {alias}")));
            }
        }

        let table_idx = self.tables.len();
        let reference = TableScopeRef { table_idx };
        let scope = Table {
            reference,
            alias,
            column_types,
            column_names,
        };
        self.tables.push(scope);

        let child = self.get_child_context_mut(idx)?;
        child.scopes.push(reference);

        Ok(reference)
    }

    pub fn push_correlation(
        &mut self,
        idx: BindContextRef,
        correlation: CorrelatedColumn,
    ) -> Result<()> {
        let child = self.get_child_context_mut(idx)?;
        child.correlated_columns.push(correlation);
        Ok(())
    }

    /// Tries to find the the table scope that has a matching column name.
    ///
    /// This will only search the current scope, and will not look at any outer
    /// scopes.
    ///
    /// Returns the table, and the relative index of the column within that table.
    pub fn find_table_scope_for_column(
        &self,
        current: BindContextRef,
        column: &str,
    ) -> Result<Option<(&Table, usize)>> {
        let mut found = None;
        for table in self.iter_table_scopes(current)? {
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

    pub fn get_table_scope(&self, scope_ref: TableScopeRef) -> Result<&Table> {
        self.tables
            .get(scope_ref.table_idx)
            .ok_or_else(|| RayexecError::new("Missing table scope"))
    }

    /// Iterate table scopes in the given bind context.
    pub fn iter_table_scopes(
        &self,
        current: BindContextRef,
    ) -> Result<impl Iterator<Item = &Table>> {
        let context = self.get_child_context(current)?;
        Ok(context
            .scopes
            .iter()
            .map(|table| &self.tables[table.table_idx]))
    }

    fn get_child_context(&self, bind_ref: BindContextRef) -> Result<&ChildBindContext> {
        let child = self
            .contexts
            .get(bind_ref.context_idx)
            .ok_or_else(|| RayexecError::new("Missing child bind context"))?;

        Ok(child)
    }

    fn get_child_context_mut(&mut self, bind_ref: BindContextRef) -> Result<&mut ChildBindContext> {
        let child = self
            .contexts
            .get_mut(bind_ref.context_idx)
            .ok_or_else(|| RayexecError::new("Missing child bind context"))?;

        Ok(child)
    }
}
