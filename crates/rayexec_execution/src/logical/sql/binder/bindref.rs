use rayexec_error::{RayexecError, Result};
use rayexec_parser::{ast, meta::Raw};
use serde::{Deserialize, Serialize};
use std::fmt;

use crate::{
    database::entry::TableEntry,
    functions::{
        aggregate::AggregateFunction,
        scalar::ScalarFunction,
        table::{PlannedTableFunction, TableFunctionArgs},
    },
};

/// Index into the bind list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BindListIdx {
    /// Index into the `bound` list.
    Bound(usize),
    /// Index into the `unbound` list.
    Unbound(usize),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MaybeBound<B, U> {
    Bound(B),
    Unbound(U),
}

/// List for holding bound and unbound variants for a single logical concept
/// (table, function, etc).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BindList<B, U> {
    pub bound: Vec<B>,
    pub unbound: Vec<U>,
}

/// Bind list for table like objects (tables or CTEs).
pub type TableBindList = BindList<TableOrCteReference, ast::ObjectReference>;

/// Bind list for functions (scalar or aggs).
pub type FunctionBindList = BindList<FunctionReference, ast::ObjectReference>;

/// Bind list for table functions.
pub type TableFunctionBindList = BindList<TableFunctionReference, ast::ObjectReference>;

/// Bind list for table function arguments.
pub type TableFunctionArgsBindList = BindList<TableFunctionArgs, Vec<ast::FunctionArg<Raw>>>;

impl<B, U> BindList<B, U> {
    pub fn any_unbound(&self) -> bool {
        !self.unbound.is_empty()
    }

    pub fn try_get_bound(&self, idx: BindListIdx) -> Result<&B> {
        match idx {
            BindListIdx::Bound(idx) => self
                .bound
                .get(idx)
                .ok_or_else(|| RayexecError::new("Missing bound item")),
            BindListIdx::Unbound(_) => Err(RayexecError::new("Invalid bind list idx variant")),
        }
    }

    pub fn push_maybe_bound(&mut self, maybe: MaybeBound<B, U>) -> BindListIdx {
        match maybe {
            MaybeBound::Bound(b) => self.push_bound(b),
            MaybeBound::Unbound(u) => self.push_unbound(u),
        }
    }

    pub fn push_bound(&mut self, bound: B) -> BindListIdx {
        let idx = self.bound.len();
        self.bound.push(bound);
        BindListIdx::Bound(idx)
    }

    pub fn push_unbound(&mut self, unbound: U) -> BindListIdx {
        let idx = self.bound.len();
        self.unbound.push(unbound);
        BindListIdx::Unbound(idx)
    }
}

impl<B, U> Default for BindList<B, U> {
    fn default() -> Self {
        Self {
            bound: Vec::new(),
            unbound: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum FunctionReference {
    Scalar(Box<dyn ScalarFunction>),
    Aggregate(Box<dyn AggregateFunction>),
}

/// Table or CTE found in the FROM clause.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TableOrCteReference {
    /// Resolved table.
    Table {
        catalog: String,
        schema: String,
        entry: TableEntry,
    },
    /// Resolved CTE.
    Cte(CteReference),
}

/// References a CTE that can be found in `BindData`.
///
/// Note that this doesn't hold the CTE itself since it may be referenced more
/// than once in a query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CteReference {
    /// Index into the CTE map.
    pub idx: usize,
}

/// Table function found in the FROM clause.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TableFunctionReference {
    /// Name of the original function.
    ///
    /// This is used to allow the user to reference the output of the function
    /// if not provided an alias.
    pub name: String,

    /// The initialized table function.
    pub func: Box<dyn PlannedTableFunction>,
}

// TODO: Figure out how we want to represent things like tables in a CREATE
// TABLE. We don't want to resolve, so a vec of strings works for now.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ItemReference(pub Vec<String>);

impl ItemReference {
    pub fn pop(&mut self) -> Result<String> {
        // TODO: Could be more informative with this error.
        self.0
            .pop()
            .ok_or_else(|| RayexecError::new("End of reference"))
    }

    pub fn pop_2(&mut self) -> Result<[String; 2]> {
        let a = self
            .0
            .pop()
            .ok_or_else(|| RayexecError::new("Expected 2 identifiers, got 0"))?;
        let b = self
            .0
            .pop()
            .ok_or_else(|| RayexecError::new("Expected 2 identifiers, got 1"))?;
        Ok([b, a])
    }

    pub fn pop_3(&mut self) -> Result<[String; 3]> {
        let a = self
            .0
            .pop()
            .ok_or_else(|| RayexecError::new("Expected 3 identifiers, got 0"))?;
        let b = self
            .0
            .pop()
            .ok_or_else(|| RayexecError::new("Expected 3 identifiers, got 1"))?;
        let c = self
            .0
            .pop()
            .ok_or_else(|| RayexecError::new("Expected 3 identifiers, got 2"))?;
        Ok([c, b, a])
    }
}

impl From<Vec<String>> for ItemReference {
    fn from(value: Vec<String>) -> Self {
        ItemReference(value)
    }
}

impl fmt::Display for ItemReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.join(","))
    }
}
