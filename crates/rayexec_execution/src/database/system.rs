use crate::{
    database::catalog::{Catalog, CatalogTx},
    functions::{aggregate::BUILTIN_AGGREGATE_FUNCTIONS, scalar::BUILTIN_SCALAR_FUNCTIONS},
};
use once_cell::sync::Lazy;
use rayexec_error::Result;

use super::{
    catalog::InMemoryCatalog,
    create::{CreateAggregateFunctionInfo, CreateScalarFunctionInfo, CreateSchemaInfo, OnConflict},
};

pub static SYSTEM_CATALOG: Lazy<InMemoryCatalog> =
    Lazy::new(|| new_system_catalog().expect("catalog to be valid"));

/// Creates a new in-memory system catalog containing all of our built in
/// functions and schemas.
fn new_system_catalog() -> Result<InMemoryCatalog> {
    let mut catalog = InMemoryCatalog::default();
    let tx = CatalogTx::new();

    catalog.create_schema(
        &tx,
        CreateSchemaInfo {
            name: "glare_catalog".into(),
            on_conflict: OnConflict::Error,
        },
    )?;
    catalog.create_schema(
        &tx,
        CreateSchemaInfo {
            name: "information_schema".into(),
            on_conflict: OnConflict::Error,
        },
    )?;
    catalog.create_schema(
        &tx,
        CreateSchemaInfo {
            name: "pg_catalog".into(),
            on_conflict: OnConflict::Error,
        },
    )?;

    let schema = catalog.get_schema_mut(&tx, "glare_catalog")?;

    // Add builtin scalars.
    for func in BUILTIN_SCALAR_FUNCTIONS.iter() {
        schema.create_scalar_function(
            &tx,
            CreateScalarFunctionInfo {
                name: func.name().to_string(),
                implementation: func.clone(),
                on_conflict: OnConflict::Error,
            },
        )?;

        for alias in func.aliases() {
            schema.create_scalar_function(
                &tx,
                CreateScalarFunctionInfo {
                    name: alias.to_string(),
                    implementation: func.clone(),
                    on_conflict: OnConflict::Error,
                },
            )?
        }
    }

    // Add builtin aggregates.
    for func in BUILTIN_AGGREGATE_FUNCTIONS.iter() {
        schema.create_aggregate_function(
            &tx,
            CreateAggregateFunctionInfo {
                name: func.name().to_string(),
                implementation: func.clone(),
                on_conflict: OnConflict::Error,
            },
        )?;

        for alias in func.aliases() {
            schema.create_aggregate_function(
                &tx,
                CreateAggregateFunctionInfo {
                    name: alias.to_string(),
                    implementation: func.clone(),
                    on_conflict: OnConflict::Error,
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
