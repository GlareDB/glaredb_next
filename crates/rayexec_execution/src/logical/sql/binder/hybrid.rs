use std::sync::Arc;

use crate::{
    database::{catalog::CatalogTx, DatabaseContext},
    datasource::FileHandlers,
    runtime::ExecutionRuntime,
};
use rayexec_error::{RayexecError, Result};

use super::{
    bindref::{MaybeBound, TableFunctionReference},
    BindData, Binder, ExpressionBinder,
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
#[derive(Debug)]
pub struct HybridResolver<'a> {
    pub binder: Binder<'a>,
}

impl<'a> HybridResolver<'a> {
    pub fn new(
        tx: &'a CatalogTx,
        context: &'a DatabaseContext,
        runtime: &'a Arc<dyn ExecutionRuntime>,
    ) -> Self {
        // Currently just use an empty file handler, all files should have been
        // resolved appropriately on the "local" side.
        //
        // This may change if:
        // - We have "remote"-only (or cloud-only) file handlers.
        // - We want to handle object store files remotely always, enabling
        //   things like automatically using credentials and stuff.
        //
        // The second point will likely be handled in a way where we replace the
        // file with the proper function on the "local" side anyways, so this
        // would still be fine being empty.
        const EMPTY_FILE_HANDLER_REF: &'static FileHandlers = &FileHandlers::empty();

        HybridResolver {
            binder: Binder::new(tx, context, EMPTY_FILE_HANDLER_REF, runtime),
        }
    }

    /// Resolve all unbound references in the bind data, erroring if anything
    /// fails to resolve.
    ///
    /// Bound items should not be checked.
    pub async fn resolve_all_unbound(&self, mut bind_data: BindData) -> Result<BindData> {
        self.resolve_unbound_table_fn(&mut bind_data).await?;
        // TODO: Tables, etc.
        // TODO: Might be worth doing these in parallel since we have the
        // complete context of the query.
        Ok(bind_data)
    }

    async fn resolve_unbound_table_fn(&self, bind_data: &mut BindData) -> Result<()> {
        for item in bind_data.table_functions.inner.iter_mut() {
            if let MaybeBound::Unbound(unbound) = item {
                // TODO: Definitely want to return a better error if we couldn't
                // find a function.
                // TODO: Reduce duplication with binder.

                let table_fn = self
                    .binder
                    .resolve_table_function(unbound.reference.clone())
                    .await?
                    .ok_or_else(|| {
                        RayexecError::new(format!(
                            "Missing table function for reference '{}'",
                            unbound.reference
                        ))
                    })?;

                let args = ExpressionBinder::new(&self.binder)
                    .bind_table_function_args(unbound.args.clone())
                    .await?;

                let name = table_fn.name().to_string();
                let func = table_fn
                    .plan_and_initialize(self.binder.runtime, args.clone())
                    .await?;

                // TODO: Marker indicating this needs to be executing remotely.
                *item = MaybeBound::Bound(TableFunctionReference { name, func, args })
            }
        }

        Ok(())
    }
}
