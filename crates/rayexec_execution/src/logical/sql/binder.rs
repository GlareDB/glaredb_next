use futures::FutureExt;
use rayexec_bullet::field::DataType;
use rayexec_error::{RayexecError, Result};
use rayexec_parser::{
    ast,
    meta::{AstMeta, Raw},
    statement::{RawStatement, Statement},
};

use crate::{
    database::{catalog::CatalogTx, DatabaseContext},
    functions::{aggregate::GenericAggregateFunction, scalar::GenericScalarFunction},
};

pub type BoundStatement = Statement<Bound>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Bound;

#[derive(Debug, Clone, PartialEq)]
pub enum BoundFunctionReference {
    Scalar(Box<dyn GenericScalarFunction>),
    Aggregate(Box<dyn GenericAggregateFunction>),
}

impl AstMeta for Bound {
    type DataSourceName = String;
    type ItemReference = String;
    type FunctionReference = BoundFunctionReference;
    type ColumnReference = String;
    type DataType = DataType;
}

#[derive(Debug)]
pub struct BindData {}

/// Binds a raw SQL AST with entries in the catalog.
#[derive(Debug)]
pub struct Binder<'a> {
    tx: &'a CatalogTx,
    context: &'a DatabaseContext,
    data: BindData,
}

impl<'a> Binder<'a> {
    pub fn new(tx: &'a CatalogTx, context: &'a DatabaseContext) -> Self {
        unimplemented!()
    }

    pub async fn bind_statement(self, stmt: RawStatement) -> Result<(BoundStatement, BindData)> {
        unimplemented!()
    }
}

struct ExpressionBinder<'a> {
    binder: &'a Binder<'a>,
}

impl<'a> ExpressionBinder<'a> {
    /// Bind an expression.
    async fn bind_expression(&self, expr: ast::Expr<Raw>) -> Result<ast::Expr<Bound>> {
        match expr {
            ast::Expr::Function(func) => {
                // TODO: Search path (with system being the first to check)
                if func.reference.0.len() != 1 {
                    return Err(RayexecError::new(
                        "Qualified function names not yet supported",
                    ));
                }
                let func_name = &func.reference.0[0].as_normalized_string();
                let catalog = "system";
                let schema = "glare_catalog";

                let filter = match func.filter {
                    Some(filter) => Some(Box::new(Box::pin(self.bind_expression(*filter)).await?)),
                    None => None,
                };

                let mut args = Vec::with_capacity(func.args.len());
                for func_arg in func.args {
                    let func_arg = match func_arg {
                        ast::FunctionArg::Named { name, arg } => ast::FunctionArg::Named {
                            name,
                            arg: Box::pin(self.bind_expression(arg)).await?,
                        },
                        ast::FunctionArg::Unnamed { arg } => ast::FunctionArg::Unnamed {
                            arg: Box::pin(self.bind_expression(arg)).await?,
                        },
                    };
                    args.push(func_arg);
                }

                // Check scalars first.
                if let Some(scalar) = self
                    .binder
                    .context
                    .get_catalog(catalog)?
                    .get_scalar_fn(self.binder.tx, schema, func_name)
                    .await?
                {
                    return Ok(ast::Expr::Function(ast::Function {
                        reference: BoundFunctionReference::Scalar(scalar),
                        args,
                        filter,
                    }));
                }

                // Now check aggregates.
                if let Some(aggregate) = self
                    .binder
                    .context
                    .get_catalog(catalog)?
                    .get_aggregate_fn(self.binder.tx, schema, func_name)
                    .await?
                {
                    return Ok(ast::Expr::Function(ast::Function {
                        reference: BoundFunctionReference::Aggregate(aggregate),
                        args,
                        filter,
                    }));
                }

                Err(RayexecError::new(format!(
                    "Cannot resolve function with name {}",
                    func.reference
                )))
            }
            _ => unimplemented!(),
        }
    }
}
