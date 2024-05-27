use crate::{
    database::{
        catalog::{Catalog, CatalogTx},
        schema::{CreateAggregateFunction, CreateScalarFunction},
    },
    functions::{aggregate::BUILTIN_AGGREGATE_FUNCTIONS, scalar::BUILTIN_SCALAR_FUNCTIONS},
};
use once_cell::sync::Lazy;
use rayexec_error::Result;

use super::catalog::InMemoryCatalog;

pub static SYSTEM_CATALOG: Lazy<InMemoryCatalog> =
    Lazy::new(|| new_system_catalog().expect("catalog to be valid"));

/// Creates a new in-memory system catalog containing all of our built in
/// functions and schemas.
fn new_system_catalog() -> Result<InMemoryCatalog> {
    let mut catalog = InMemoryCatalog::default();
    let tx = CatalogTx::new();

    catalog.create_schema(&tx, "glare_catalog")?;
    catalog.create_schema(&tx, "information_schema")?;

    let schema = catalog.get_schema_mut(&tx, "glare_catalog")?;

    // Add builtin scalars.
    for func in BUILTIN_SCALAR_FUNCTIONS.iter() {
        schema.create_scalar_function(
            &tx,
            CreateScalarFunction {
                name: func.name().to_string(),
                implementation: func.clone(),
            },
        )?;

        for alias in func.aliases() {
            schema.create_scalar_function(
                &tx,
                CreateScalarFunction {
                    name: alias.to_string(),
                    implementation: func.clone(),
                },
            )?
        }
    }

    // Add builtin aggregates.
    for func in BUILTIN_AGGREGATE_FUNCTIONS.iter() {
        schema.create_aggregate_function(
            &tx,
            CreateAggregateFunction {
                name: func.name().to_string(),
                implementation: func.clone(),
            },
        )?;

        for alias in func.aliases() {
            schema.create_aggregate_function(
                &tx,
                CreateAggregateFunction {
                    name: alias.to_string(),
                    implementation: func.clone(),
                },
            )?
        }
    }

    Ok(catalog)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_create_system_catalog() {
        new_system_catalog().unwrap();
    }
}
