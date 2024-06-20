use std::collections::HashMap;

use super::{
    aggregate::AggregatePlanner,
    binder::{BindData, Bound, BoundCteReference, BoundTableOrCteReference},
    expr::{ExpandedSelectExpr, ExpressionContext},
    scope::{ColumnRef, FromScope, TableReference},
    subquery::SubqueryPlanner,
};
use crate::{
    database::{
        create::OnConflict,
        drop::{DropInfo, DropObject},
    },
    engine::vars::SessionVars,
    logical::{
        operator::{
            AnyJoin, AttachDatabase, CreateSchema, CreateTable, CrossJoin, Describe,
            DetachDatabase, DropEntry, Explain, ExplainFormat, ExpressionList, Filter, Insert,
            JoinType, Limit, LogicalExpression, LogicalOperator, Order, OrderByExpr, Projection,
            ResetVar, Scan, SetVar, ShowVar, TableFunction, VariableOrAll,
        },
        sql::query::QueryNodePlanner,
    },
};
use rayexec_bullet::field::{Field, Schema, TypeSchema};
use rayexec_error::{RayexecError, Result};
use rayexec_parser::{
    ast::{self, OrderByNulls, OrderByType, QueryNode},
    statement::Statement,
};
use tracing::trace;

const EMPTY_SCOPE: &FromScope = &FromScope::empty();
const EMPTY_TYPE_SCHEMA: &TypeSchema = &TypeSchema::empty();

#[derive(Debug)]
pub struct LogicalQuery {
    /// Root of the query.
    pub root: LogicalOperator,

    /// The final scope of the query.
    pub scope: FromScope,
}

#[derive(Debug, Clone)]
pub struct PlanContext<'a> {
    /// Session variables.
    pub vars: &'a SessionVars,

    /// Scopes outside this context.
    pub outer_scopes: Vec<FromScope>,

    pub bind_data: &'a BindData,
}

impl<'a> PlanContext<'a> {
    pub fn new(vars: &'a SessionVars, bind_data: &'a BindData) -> Self {
        PlanContext {
            vars,
            outer_scopes: Vec::new(),
            bind_data,
        }
    }

    pub fn plan_statement(mut self, stmt: Statement<Bound>) -> Result<LogicalQuery> {
        trace!("planning statement");
        match stmt {
            Statement::Explain(explain) => {
                let mut planner = QueryNodePlanner::new(self.bind_data);
                let plan = match explain.body {
                    ast::ExplainBody::Query(query) => planner.plan_query(query)?,
                };
                let format = match explain.output {
                    Some(ast::ExplainOutput::Text) => ExplainFormat::Text,
                    Some(ast::ExplainOutput::Json) => ExplainFormat::Json,
                    None => ExplainFormat::Text,
                };
                Ok(LogicalQuery {
                    root: LogicalOperator::Explain(Explain {
                        analyze: explain.analyze,
                        verbose: explain.verbose,
                        format,
                        input: Box::new(plan.root),
                    }),
                    scope: FromScope::empty(),
                })
            }
            Statement::Query(query) => {
                let mut planner = QueryNodePlanner::new(self.bind_data);
                planner.plan_query(query)
            }
            Statement::CreateTable(create) => self.plan_create_table(create),
            Statement::CreateSchema(create) => self.plan_create_schema(create),
            Statement::Drop(drop) => self.plan_drop(drop),
            Statement::Insert(insert) => self.plan_insert(insert),
            Statement::SetVariable(ast::SetVariable {
                mut reference,
                value,
            }) => {
                let planner = QueryNodePlanner::new(self.bind_data);
                let expr_ctx = ExpressionContext::new(&planner, EMPTY_SCOPE, EMPTY_TYPE_SCHEMA);
                let expr = expr_ctx.plan_expression(value)?;
                Ok(LogicalQuery {
                    root: LogicalOperator::SetVar(SetVar {
                        name: reference.pop()?, // TODO: Allow compound references?
                        value: expr.try_into_scalar()?,
                    }),
                    scope: FromScope::empty(),
                })
            }
            Statement::ShowVariable(ast::ShowVariable { mut reference }) => {
                let name = reference.pop()?; // TODO: Allow compound references?
                let var = self.vars.get_var(&name)?;
                let scope = FromScope::with_columns(None, [name.clone()]);
                Ok(LogicalQuery {
                    root: LogicalOperator::ShowVar(ShowVar { var: var.clone() }),
                    scope,
                })
            }
            Statement::ResetVariable(ast::ResetVariable { var }) => {
                let var = match var {
                    ast::VariableOrAll::Variable(mut v) => {
                        let name = v.pop()?; // TODO: Allow compound references?
                        let var = self.vars.get_var(&name)?;
                        VariableOrAll::Variable(var.clone())
                    }
                    ast::VariableOrAll::All => VariableOrAll::All,
                };
                Ok(LogicalQuery {
                    root: LogicalOperator::ResetVar(ResetVar { var }),
                    scope: FromScope::empty(),
                })
            }
            Statement::Attach(attach) => self.plan_attach(attach),
            Statement::Detach(detach) => self.plan_detach(detach),
            Statement::Describe(describe) => {
                let mut planner = QueryNodePlanner::new(self.bind_data);
                let plan = match describe {
                    ast::Describe::Query(query) => planner.plan_query(query)?,
                    ast::Describe::FromNode(from) => {
                        planner.plan_from_node(from, FromScope::empty())?
                    }
                };

                let type_schema = plan.root.output_schema(&[])?; // TODO: Include outer schema
                debug_assert_eq!(plan.scope.num_columns(), type_schema.types.len());

                let schema = Schema::new(
                    plan.scope
                        .items
                        .into_iter()
                        .zip(type_schema.types)
                        .map(|(item, typ)| Field::new(item.column, typ, true)),
                );

                Ok(LogicalQuery {
                    root: LogicalOperator::Describe(Describe { schema }),
                    scope: FromScope::with_columns(None, ["column_name", "datatype"]),
                })
            }
        }
    }

    fn plan_attach(&mut self, mut attach: ast::Attach<Bound>) -> Result<LogicalQuery> {
        match attach.attach_type {
            ast::AttachType::Database => {
                let mut options = HashMap::new();
                let planner = QueryNodePlanner::new(self.bind_data);
                let expr_ctx = ExpressionContext::new(&planner, EMPTY_SCOPE, EMPTY_TYPE_SCHEMA);

                for (k, v) in attach.options {
                    let k = k.into_normalized_string();
                    let v = match expr_ctx.plan_expression(v)? {
                        LogicalExpression::Literal(v) => v,
                        other => {
                            return Err(RayexecError::new(format!(
                                "Non-literal expression provided as value: {other:?}"
                            )))
                        }
                    };
                    if options.contains_key(&k) {
                        return Err(RayexecError::new(format!(
                            "Option '{k}' provided more than once"
                        )));
                    }
                    options.insert(k, v);
                }

                if attach.alias.0.len() != 1 {
                    return Err(RayexecError::new(format!(
                        "Expected a single identifier, got '{}'",
                        attach.alias
                    )));
                }
                let name = attach.alias.pop()?;
                let datasource = attach.datasource_name;

                Ok(LogicalQuery {
                    root: LogicalOperator::AttachDatabase(AttachDatabase {
                        datasource,
                        name,
                        options,
                    }),
                    scope: FromScope::empty(),
                })
            }
            ast::AttachType::Table => Err(RayexecError::new("Attach tables not yet supported")),
        }
    }

    fn plan_detach(&mut self, mut detach: ast::Detach<Bound>) -> Result<LogicalQuery> {
        match detach.attach_type {
            ast::AttachType::Database => {
                if detach.alias.0.len() != 1 {
                    return Err(RayexecError::new(format!(
                        "Expected a single identifier, got '{}'",
                        detach.alias
                    )));
                }
                let name = detach.alias.pop()?;

                Ok(LogicalQuery {
                    root: LogicalOperator::DetachDatabase(DetachDatabase { name }),
                    scope: FromScope::empty(),
                })
            }
            ast::AttachType::Table => Err(RayexecError::new("Detach tables not yet supported")),
        }
    }

    fn plan_insert(&mut self, insert: ast::Insert<Bound>) -> Result<LogicalQuery> {
        let mut planner = QueryNodePlanner::new(self.bind_data);
        let source = planner.plan_query(insert.source)?;

        let entry = match insert.table {
            BoundTableOrCteReference::Table { entry, .. } => entry,
            BoundTableOrCteReference::Cte(_) => {
                return Err(RayexecError::new("Cannot insert into CTE"))
            }
        };

        // TODO: Handle specified columns. If provided, insert a projection that
        // maps the columns to the right position.

        Ok(LogicalQuery {
            root: LogicalOperator::Insert(Insert {
                table: entry,
                input: Box::new(source.root),
            }),
            scope: FromScope::empty(),
        })
    }

    fn plan_drop(&mut self, mut drop: ast::DropStatement<Bound>) -> Result<LogicalQuery> {
        match drop.drop_type {
            ast::DropType::Schema => {
                let [catalog, schema] = drop.name.pop_2()?;

                // Dropping defaults to restricting (erroring) on dependencies.
                let deps = drop.deps.unwrap_or(ast::DropDependents::Restrict);

                let plan = LogicalOperator::Drop(DropEntry {
                    info: DropInfo {
                        catalog,
                        schema,
                        object: DropObject::Schema,
                        cascade: ast::DropDependents::Cascade == deps,
                        if_exists: drop.if_exists,
                    },
                });

                Ok(LogicalQuery {
                    root: plan,
                    scope: FromScope::empty(),
                })
            }
            _other => unimplemented!(),
        }
    }

    fn plan_create_schema(&mut self, mut create: ast::CreateSchema<Bound>) -> Result<LogicalQuery> {
        let on_conflict = if create.if_not_exists {
            OnConflict::Ignore
        } else {
            OnConflict::Error
        };

        let [catalog, schema] = create.name.pop_2()?;

        Ok(LogicalQuery {
            root: LogicalOperator::CreateSchema(CreateSchema {
                catalog,
                name: schema,
                on_conflict,
            }),
            scope: FromScope::empty(),
        })
    }

    fn plan_create_table(&mut self, mut create: ast::CreateTable<Bound>) -> Result<LogicalQuery> {
        let on_conflict = match (create.or_replace, create.if_not_exists) {
            (true, false) => OnConflict::Replace,
            (false, true) => OnConflict::Ignore,
            (false, false) => OnConflict::Error,
            (true, true) => {
                return Err(RayexecError::new(
                    "Cannot specify both OR REPLACE and IF NOT EXISTS",
                ))
            }
        };

        // TODO: Verify column constraints.
        let mut columns: Vec<_> = create
            .columns
            .into_iter()
            .map(|col| Field::new(col.name, col.datatype, true))
            .collect();

        let input = match create.source {
            Some(source) => {
                // If we have an input to the table, adjust the column definitions for the table
                // to be the output schema of the input.

                // TODO: We could allow this though. We'd just need to do some
                // projections as necessary.
                if !columns.is_empty() {
                    return Err(RayexecError::new(
                        "Cannot specify columns when running CREATE TABLE ... AS ...",
                    ));
                }

                let mut planner = QueryNodePlanner::new(self.bind_data);
                let input = planner.plan_query(source)?;
                let type_schema = input.root.output_schema(&[])?; // Source input to table should not depend on any outer queries.

                if type_schema.types.len() != input.scope.items.len() {
                    // An "us" bug. These should be the same lengths.
                    return Err(RayexecError::new(
                        "Output scope and type schemas differ in lengths",
                    ));
                }

                let fields: Vec<_> = input
                    .scope
                    .items
                    .iter()
                    .zip(type_schema.types)
                    .map(|(item, typ)| Field::new(&item.column, typ, true))
                    .collect();

                // Update columns to the fields we've generated from the input.
                columns = fields;

                Some(Box::new(input.root))
            }
            None => None,
        };

        let [catalog, schema, name] = create.name.pop_3()?;

        Ok(LogicalQuery {
            root: LogicalOperator::CreateTable(CreateTable {
                catalog,
                schema,
                name,
                columns,
                on_conflict,
                input,
            }),
            scope: FromScope::empty(),
        })
    }
}
