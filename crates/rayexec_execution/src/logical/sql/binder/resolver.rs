use crate::{
    database::{catalog::CatalogTx, DatabaseContext},
    functions::table::TableFunction,
};
use rayexec_error::{RayexecError, Result};
use rayexec_parser::ast;

use super::{
    bindref::{MaybeBound, TableFunctionReference},
    BindData,
};

/// Resolver for resolving partially bound statements.
///
/// The database context provided on this does not need to match the database
/// context that was used during the intial binding. The use case is to allow
/// the "local" session to partially bind a query, serialize the query, then
/// have the remote side complete the binding.
///
/// This allows for two instances with differently registered data source to
/// both work on query planning.
///
/// For example, the following query would typically fail in when running in
/// wasm:
///
/// SELECT * FROM read_postgres(...) INNER JOIN 'myfile.csv' ON ...
///
/// This is because we don't register the postgres data source in the wasm
/// bindings because we can't actually connect to postgres in the browser.
/// However with hyrbid execution (and this resolver), the wasm session is able
/// to bind everything _but_ the `read_postgres` call, then send the serialized
/// plan to remote node, which then uses this resolver to appropriately bind the
/// `read_postgres` function (assuming the remote node has the postgres data
/// source registered).
///
/// Once resolved, the remote node can continue with planning the statement,
/// sending back parts of the pipeline that the "local" side should execute.
#[derive(Debug, Clone)]
pub struct HybridResolver<'a> {
    pub tx: &'a CatalogTx,
    pub context: &'a DatabaseContext,
}

impl<'a> HybridResolver<'a> {
    /// Resolve all unbound references in the bind data, erroring if anything
    /// fails to resolve.
    ///
    /// Bound items should not be checked.
    pub async fn resolve_all_unbound(&self, mut bind_data: BindData) -> Result<BindData> {
        unimplemented!()
    }

    async fn resolve_unbound_table_func(&self, bind_data: &mut BindData) -> Result<()> {
        for item in bind_data.table_functions.inner.iter_mut() {
            if let MaybeBound::Unbound(unbound) = item {
                // TODO: Definitely want to return a better error if we couldn't
                // find a function.
                let func = resolve_table_function(self.tx, self.context, unbound.clone())
                    .await?
                    .try_unwrap_bound()?;

                // TODO
                // *item = MaybeBound::Bound(TableFunctionReference {
                //     name: func.name().to_string(),
                //     func,
                // });
            }
        }

        Ok(())
    }
}

pub async fn resolve_table_function(
    tx: &CatalogTx,
    context: &DatabaseContext,
    reference: ast::ObjectReference,
) -> Result<MaybeBound<Box<dyn TableFunction>, ast::ObjectReference>> {
    // TODO: Search path.
    let [catalog, schema, name] = match reference.0.len() {
        1 => [
            "system".to_string(),
            "glare_catalog".to_string(),
            reference.0[0].as_normalized_string(),
        ],
        2 => {
            let name = reference.0[1].as_normalized_string();
            let schema = reference.0[0].as_normalized_string();
            ["system".to_string(), schema, name]
        }
        3 => {
            let name = reference.0[2].as_normalized_string();
            let schema = reference.0[1].as_normalized_string();
            let catalog = reference.0[0].as_normalized_string();
            [catalog, schema, name]
        }
        _ => {
            return Err(RayexecError::new(
                "Unexpected number of identifiers in table function reference",
            ))
        }
    };

    let ent = context
        .get_catalog(&catalog)?
        .get_table_fn(tx, &schema, &name)?;

    match ent {
        Some(ent) => Ok(MaybeBound::Bound(ent)),
        None => Ok(MaybeBound::Unbound(reference)),
    }
}
