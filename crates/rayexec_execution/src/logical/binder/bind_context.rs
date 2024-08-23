use rayexec_bullet::datatype::DataType;
use rayexec_error::{RayexecError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BindContextIdx(pub usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableScopeIdx(pub BindContextIdx, pub usize);

#[derive(Debug)]
pub struct BindContext {
    contexts: Vec<ChildBindContext>,
}

#[derive(Debug)]
pub struct ChildBindContext {
    /// Index to the parent bind context.
    ///
    /// Will be None if this is the root context.
    parent: Option<BindContextIdx>,

    /// Scopes currently at this depth.
    scopes: Vec<TableScope>,
}

#[derive(Debug)]
pub struct TableScope {
    pub alias: String,
    pub column_types: Vec<DataType>,
    pub column_names: Vec<String>,
}

impl BindContext {
    pub fn new_child(&mut self, current: BindContextIdx) -> BindContextIdx {
        let idx = self.contexts.len();
        self.contexts.push(ChildBindContext {
            parent: Some(current),
            scopes: Vec::new(),
        });

        BindContextIdx(idx)
    }

    pub fn push_table_scope(
        &mut self,
        idx: BindContextIdx,
        alias: impl Into<String>,
        column_types: Vec<DataType>,
        column_names: Vec<String>,
    ) -> Result<TableScopeIdx> {
        let child = self.get_child_context_mut(idx)?;
        let alias = alias.into();

        for scope in &child.scopes {
            if scope.alias == alias {
                return Err(RayexecError::new(format!("Duplicate table name: {alias}")));
            }
        }

        let scope = TableScope {
            alias,
            column_types,
            column_names,
        };

        let scope_idx = child.scopes.len();
        child.scopes.push(scope);

        Ok(TableScopeIdx(idx, scope_idx))
    }

    /// Tries to find the the table scope that has a matching column name.
    ///
    /// This will only search the current scope, and will not look at any outer
    /// scopes.
    pub fn find_table_scope_for_column(
        &self,
        current: BindContextIdx,
        column: &str,
    ) -> Result<Option<&TableScope>> {
        let context = self.get_child_context(current)?;
        let mut found = None;
        for scope in &context.scopes {
            for col_name in &scope.column_names {
                if col_name == column {
                    if found.is_some() {
                        return Err(RayexecError::new(format!("Ambiguous column name {column}")));
                    }
                    found = Some(scope);
                }
            }
        }

        Ok(found)
    }

    pub fn get_table_scope(
        &self,
        TableScopeIdx(context_idx, table_idx): TableScopeIdx,
    ) -> Result<&TableScope> {
        let context = self.get_child_context(context_idx)?;
        context
            .scopes
            .get(table_idx)
            .ok_or_else(|| RayexecError::new("Missing table scope"))
    }

    pub fn iter_table_scopes(
        &self,
        current: BindContextIdx,
    ) -> Result<impl Iterator<Item = &TableScope>> {
        let context = self.get_child_context(current)?;
        Ok(context.scopes.iter())
    }

    fn get_child_context(&self, BindContextIdx(idx): BindContextIdx) -> Result<&ChildBindContext> {
        self.contexts
            .get(idx)
            .ok_or_else(|| RayexecError::new("Missing child bind context"))
    }

    fn get_child_context_mut(
        &mut self,
        BindContextIdx(idx): BindContextIdx,
    ) -> Result<&mut ChildBindContext> {
        self.contexts
            .get_mut(idx)
            .ok_or_else(|| RayexecError::new("Missing child bind context"))
    }
}
