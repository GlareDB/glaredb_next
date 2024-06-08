pub mod series;

use dyn_clone::DynClone;
use futures::future::BoxFuture;
use once_cell::sync::Lazy;
use rayexec_bullet::field::Schema;
use rayexec_bullet::scalar::OwnedScalarValue;
use rayexec_error::Result;
use std::{collections::HashMap, fmt::Debug};

use crate::database::table::DataTable;

pub static BUILTIN_TABLE_FUNCTIONS: Lazy<Vec<Box<dyn GenericTableFunction>>> = Lazy::new(Vec::new);

#[derive(Debug, Clone, PartialEq)]
pub struct TableFunctionArgs {
    pub named: HashMap<String, OwnedScalarValue>,
    pub positional: Vec<OwnedScalarValue>,
}

/// A generic table function provides a way to dispatch to a more specialized
/// table functions.
///
/// For example, the generic function 'read_csv' might have specialized versions
/// for reading a csv from the local file system, another for reading from
/// object store, etc.
///
/// The specialized variant should be determined by function argument inputs.
pub trait GenericTableFunction: Debug + Sync + Send + DynClone {
    /// Name of the function.
    fn name(&self) -> &str;

    /// Optional aliases for this function.
    fn aliases(&self) -> &[&str] {
        &[]
    }

    /// Return the specialized function to use based on the function arguments.
    fn specialize(&self, args: &TableFunctionArgs) -> Result<Box<dyn SpecializedTableFunction>>;
}

impl Clone for Box<dyn GenericTableFunction> {
    fn clone(&self) -> Self {
        dyn_clone::clone_box(&**self)
    }
}

impl PartialEq<dyn GenericTableFunction> for Box<dyn GenericTableFunction + '_> {
    fn eq(&self, other: &dyn GenericTableFunction) -> bool {
        self.as_ref() == other
    }
}

impl PartialEq for dyn GenericTableFunction + '_ {
    fn eq(&self, other: &dyn GenericTableFunction) -> bool {
        self.name() == other.name()
    }
}

pub trait SpecializedTableFunction: Debug + Sync + Send + DynClone {
    /// Get the schema for the function output.
    fn schema(&mut self) -> BoxFuture<'static, Result<Schema>>;

    /// Return a data table representing the function output.
    fn datatable(&mut self) -> Result<Box<dyn DataTable>>;
}
