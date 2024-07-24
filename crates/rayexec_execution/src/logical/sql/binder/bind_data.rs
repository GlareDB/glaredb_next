use rayexec_error::{RayexecError, Result};
use rayexec_parser::ast::{self, QueryNode};
use serde::{
    de::{self, DeserializeSeed, Visitor},
    ser::{SerializeMap, SerializeTupleVariant},
    Deserialize, Deserializer, Serialize, Serializer,
};
use std::fmt;

use crate::{
    database::{entry::TableEntry, DatabaseContext},
    functions::{
        aggregate::AggregateFunction,
        scalar::ScalarFunction,
        table::{PlannedTableFunction, TableFunction, TableFunctionArgs},
    },
    logical::operator::LocationRequirement,
    serde::{AggregateFunctionDeserializer, ScalarFunctionDeserializer},
};

use super::Bound;

/// Data that's collected during binding, including resolved tables, functions,
/// and other database objects.
///
/// Planning will reference these items directly instead of having to resolve
/// them.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct BindData {
    /// A bound table may reference either an actual table, or a CTE. An unbound
    /// reference may only reference a table.
    pub tables: BindList<BoundTableOrCteReference, ast::ObjectReference>,

    /// Bound scalar or aggregate functions.
    pub functions: BindList<BoundFunctionReference, ast::ObjectReference>,

    /// Bound (and planned) table functions. Unbound table functions include the
    /// table function arguments to allow for quick planning on the remote side.
    // TODO: This may change to just have `dyn TableFunction` references, and
    // then have a separate step after binding that initializes all table
    // functions.
    pub table_functions: BindList<BoundTableFunctionReference, UnboundTableFunctionReference>,

    /// How "deep" in the plan are we.
    ///
    /// Incremented everytime we dive into a subquery.
    ///
    /// This provides a primitive form of scoping for CTE resolution.
    pub current_depth: usize,

    /// CTEs are appended to the vec as they're encountered.
    ///
    /// When search for a CTE, the vec should be iterated from right to left to
    /// try to get the "closest" CTE to the reference.
    pub ctes: Vec<BoundCte>,
}

impl BindData {
    /// Checks if there's any unbound references in this query's bind data.
    pub fn any_unbound(&self) -> bool {
        self.tables.any_unbound()
            || self.functions.any_unbound()
            || self.table_functions.any_unbound()
    }

    /// Try to find a CTE by its normalized name.
    ///
    /// This will iterate the cte vec right to left to find best cte that
    /// matches this name.
    ///
    /// The current depth will be used to determine if a CTE is valid to
    /// reference or not. What this means is as we iterate, we can go "up" in
    /// depth, but never back down, as going back down would mean we're
    /// attempting to resolve a cte from a "sibling" subquery.
    // TODO: This doesn't account for CTEs defined in sibling subqueries yet
    // that happen to have the same name and depths _and_ there's no CTEs in the
    // parent.
    pub fn find_cte(&self, name: &str) -> Option<CteReference> {
        let mut search_depth = self.current_depth;

        for (idx, cte) in self.ctes.iter().rev().enumerate() {
            if cte.depth > search_depth {
                // We're looking another subquery's CTEs.
                return None;
            }

            if cte.name == name {
                // We found a good reference.
                return Some(CteReference {
                    idx: (self.ctes.len() - 1) - idx, // Since we're iterating backwards.
                });
            }

            // Otherwise keep searching, even if the cte is up a level.
            search_depth = cte.depth;
        }

        // No CTE found.
        None
    }

    pub fn inc_depth(&mut self) {
        self.current_depth += 1
    }

    pub fn dec_depth(&mut self) {
        self.current_depth -= 1;
    }

    /// Push a CTE into bind data, returning a CTE reference.
    pub fn push_cte(&mut self, cte: BoundCte) -> CteReference {
        let idx = self.ctes.len();
        self.ctes.push(cte);
        CteReference { idx }
    }
}

/// A bound aggregate or scalar function.
#[derive(Debug, Clone, PartialEq)]
pub enum BoundFunctionReference {
    Scalar(Box<dyn ScalarFunction>),
    Aggregate(Box<dyn AggregateFunction>),
}

impl Serialize for BoundFunctionReference {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut ser = serializer.serialize_map(Some(1))?;
        match self {
            Self::Scalar(scalar) => ser.serialize_entry("scalar", scalar)?,
            Self::Aggregate(agg) => ser.serialize_entry("aggregate", agg)?,
        }
        ser.end()
    }
}

struct BoundFunctionReferenceDeserializer<'a> {
    context: &'a DatabaseContext,
}

impl<'de, 'a> DeserializeSeed<'de> for BoundFunctionReferenceDeserializer<'a> {
    type Value = BoundFunctionReference;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct MapVisitor<'a> {
            context: &'a DatabaseContext,
        }

        impl<'de, 'a> Visitor<'de> for MapVisitor<'a> {
            type Value = BoundFunctionReference;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                write!(formatter, "a map")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: de::MapAccess<'de>,
            {
                match map.next_key()? {
                    Some("scalar") => {
                        let scalar = map.next_value_seed(ScalarFunctionDeserializer {
                            context: self.context,
                        })?;
                        Ok(BoundFunctionReference::Scalar(scalar))
                    }
                    Some("aggregate") => {
                        let agg = map.next_value_seed(AggregateFunctionDeserializer {
                            context: self.context,
                        })?;
                        Ok(BoundFunctionReference::Aggregate(agg))
                    }
                    Some(other) => Err(de::Error::custom(format!("invalid key: {other}"))),
                    None => Err(de::Error::custom("missing key")),
                }
            }
        }

        deserializer.deserialize_map(MapVisitor {
            context: self.context,
        })
    }
}

/// A bound table function reference.
#[derive(Debug, Clone, PartialEq)]
pub struct BoundTableFunctionReference {
    /// Name of the original function.
    ///
    /// This is used to allow the user to reference the output of the function
    /// if not provided an alias.
    pub name: String,
    /// The planned table function.
    pub func: Box<dyn PlannedTableFunction>,
    // TODO: Maybe keep args here?
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UnboundTableFunctionReference {
    /// Original reference in the ast.
    pub reference: ast::ObjectReference,
    /// Arguments to the function.
    ///
    /// Note that these are required to be constant and so we don't need to
    /// delay binding.
    pub args: TableFunctionArgs,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MaybeBound<B, U> {
    /// The object has been bound, and has a given location requirement.
    Bound(B, LocationRequirement),
    /// Object is unbound.
    Unbound(U),
}

impl<B, U> MaybeBound<B, U> {
    pub const fn is_bound(&self) -> bool {
        matches!(self, MaybeBound::Bound(_, _))
    }

    pub fn try_unwrap_bound(self) -> Result<(B, LocationRequirement)> {
        match self {
            Self::Bound(b, loc) => Ok((b, loc)),
            Self::Unbound(_) => Err(RayexecError::new("Bind reference is not bound")),
        }
    }
}

/// List for holding bound and unbound variants for a single logical concept
/// (table, function, etc).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BindList<B, U> {
    pub inner: Vec<MaybeBound<B, U>>,
}

/// Index into the bind list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BindListIdx(pub usize);

impl<B, U> BindList<B, U> {
    pub fn any_unbound(&self) -> bool {
        self.inner
            .iter()
            .any(|v| matches!(v, MaybeBound::Unbound(_)))
    }

    pub fn try_get_bound(&self, idx: BindListIdx) -> Result<(&B, LocationRequirement)> {
        match self.inner.get(idx.0) {
            Some(MaybeBound::Bound(b, loc)) => Ok((b, *loc)),
            Some(MaybeBound::Unbound(_)) => Err(RayexecError::new("Item not bound")),
            None => Err(RayexecError::new("Missing bind item")),
        }
    }

    pub fn push_maybe_bound(&mut self, maybe: MaybeBound<B, U>) -> BindListIdx {
        let idx = self.inner.len();
        self.inner.push(maybe);
        BindListIdx(idx)
    }

    pub fn push_bound(&mut self, bound: B, loc: LocationRequirement) -> BindListIdx {
        self.push_maybe_bound(MaybeBound::Bound(bound, loc))
    }

    pub fn push_unbound(&mut self, unbound: U) -> BindListIdx {
        self.push_maybe_bound(MaybeBound::Unbound(unbound))
    }
}

impl<B, U> Default for BindList<B, U> {
    fn default() -> Self {
        Self { inner: Vec::new() }
    }
}

/// Table or CTE found in the FROM clause.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BoundTableOrCteReference {
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

// TODO: This might need some scoping information.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BoundCte {
    /// Normalized name for the CTE.
    pub name: String,
    /// Depth this CTE was found at.
    pub depth: usize,
    /// Column aliases taken directly from the ast.
    pub column_aliases: Option<Vec<ast::Ident>>,
    /// The bound query node.
    pub body: QueryNode<Bound>,
    /// If this CTE should be materialized.
    pub materialized: bool,
}

#[cfg(test)]
mod tests {
    use crate::{
        database::storage::system::SystemCatalog,
        datasource::DataSourceRegistry,
        functions::{aggregate::sum::Sum, scalar::string::Repeat},
    };

    use super::*;

    #[test]
    fn round_trip_bound_function() {
        let context = DatabaseContext::new(SystemCatalog::new(&DataSourceRegistry::default()));

        let bound_scalar = BoundFunctionReference::Scalar(Box::new(Repeat));
        let serialized = serde_json::to_string(&bound_scalar).unwrap();
        let deserializer = BoundFunctionReferenceDeserializer { context: &context };
        let got_scalar = deserializer
            .deserialize(&mut serde_json::Deserializer::from_str(&serialized))
            .unwrap();

        assert_eq!(bound_scalar, got_scalar);

        let bound_agg = BoundFunctionReference::Aggregate(Box::new(Sum));
        let serialized = serde_json::to_string(&bound_agg).unwrap();
        let deserializer = BoundFunctionReferenceDeserializer { context: &context };
        let got_agg = deserializer
            .deserialize(&mut serde_json::Deserializer::from_str(&serialized))
            .unwrap();

        assert_eq!(bound_agg, got_agg);

        assert_ne!(bound_scalar, got_agg);
        assert_ne!(bound_agg, got_scalar);
    }
}
