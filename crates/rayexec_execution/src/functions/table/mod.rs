use dyn_clone::DynClone;
use once_cell::sync::Lazy;
use rayexec_bullet::field::Schema;
use rayexec_bullet::scalar::OwnedScalarValue;
use rayexec_bullet::{array::Array, field::DataType};
use rayexec_error::{RayexecError, Result};
use std::sync::Arc;
use std::{collections::HashMap, fmt::Debug};

use crate::database::table::DataTableScan;

use super::{ReturnType, Signature};

pub static BUILTIN_TABLE_FUNCTIONS: Lazy<Vec<Box<dyn GenericTableFunction>>> = Lazy::new(|| vec![]);

#[derive(Debug)]
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

// TODO: Don't think this is amazing yet.
pub trait SpecializedTableFunction: Debug + Sync + Send + DynClone {
    fn load_schema(&mut self) -> Result<Schema>;
    fn scan(&self, num_partitions: usize) -> Result<Vec<Box<dyn DataTableScan>>>;
}
