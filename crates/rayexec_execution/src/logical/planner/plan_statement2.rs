use std::collections::HashMap;

use super::scope::Scope;
use crate::{
    database::{
        create::OnConflict,
        drop::{DropInfo, DropObject},
    },
    engine::vars::SessionVars,
    logical::{
        context::QueryContext,
        expr::LogicalExpression,
        operator::{
            AttachDatabase, CopyTo, CreateSchema, CreateTable, Describe, DetachDatabase, DropEntry,
            Explain, ExplainFormat, Insert, LocationRequirement, LogicalNode, LogicalOperator,
            Projection, ResetVar, Scan, SetVar, ShowVar, VariableOrAll,
        },
        planner::{plan_expr::ExpressionContext, plan_query2::QueryNodePlanner},
        resolver::{
            resolve_context::ResolveContext, resolved_table::ResolvedTableOrCteReference,
            ResolvedMeta,
        },
    },
};
use rayexec_bullet::field::{Field, Schema, TypeSchema};
use rayexec_error::{not_implemented, RayexecError, Result};
use rayexec_parser::{ast, statement::Statement};
use tracing::trace;

const EMPTY_SCOPE: &Scope = &Scope::empty();
const EMPTY_TYPE_SCHEMA: &TypeSchema = &TypeSchema::empty();

#[derive(Debug)]
pub struct LogicalQuery2 {
    /// Root of the query.
    pub root: LogicalOperator,
    /// The final scope of the query.
    pub scope: Scope,
}

impl LogicalQuery2 {
    pub fn schema(&self) -> Result<Schema> {
        let type_schema = self.root.output_schema(&[])?;
        debug_assert_eq!(self.scope.num_columns(), type_schema.types.len());

        let schema = Schema::new(
            self.scope
                .items
                .iter()
                .zip(type_schema.types)
                .map(|(item, typ)| Field::new(item.column.clone(), typ, true)),
        );

        Ok(schema)
    }
}

#[derive(Debug, Clone)]
pub struct StatementPlanner2<'a> {
    /// Session variables.
    pub vars: &'a SessionVars,
    pub bind_data: &'a ResolveContext,
}

impl<'a> StatementPlanner2<'a> {
    pub fn new(vars: &'a SessionVars, bind_data: &'a ResolveContext) -> Self {
        StatementPlanner2 { vars, bind_data }
    }

    pub fn plan_statement(
        mut self,
        stmt: Statement<ResolvedMeta>,
    ) -> Result<(LogicalQuery2, QueryContext)> {
        trace!("planning statement");
        let mut context = QueryContext::new();
        let query = match stmt {
            Statement::Explain(explain) => {
                let mut planner = QueryNodePlanner::new(self.bind_data);
                let plan = match explain.body {
                    ast::ExplainBody::Query(query) => planner.plan_query(&mut context, query)?,
                };
                let format = match explain.output {
                    Some(ast::ExplainOutput::Text) => ExplainFormat::Text,
                    Some(ast::ExplainOutput::Json) => ExplainFormat::Json,
                    None => ExplainFormat::Text,
                };
                LogicalQuery2 {
                    root: LogicalOperator::Explain2(LogicalNode::new(Explain {
                        analyze: explain.analyze,
                        verbose: explain.verbose,
                        format,
                        input: Box::new(plan.root),
                    })),
                    scope: Scope::with_columns(None, ["plan_type", "plan"]),
                }
            }
            Statement::Query(query) => {
                let mut planner = QueryNodePlanner::new(self.bind_data);
                planner.plan_query(&mut context, query)?
            }
            Statement::CopyTo(copy_to) => self.plan_copy_to(&mut context, copy_to)?,
            Statement::CreateTable(create) => self.plan_create_table(&mut context, create)?,
            Statement::CreateSchema(create) => self.plan_create_schema(create)?,
            Statement::Drop(drop) => self.plan_drop(drop)?,
            Statement::Insert(insert) => self.plan_insert(&mut context, insert)?,
            Statement::SetVariable(ast::SetVariable {
                mut reference,
                value,
            }) => {
                let planner = QueryNodePlanner::new(self.bind_data);
                let expr_ctx = ExpressionContext::new(&planner, EMPTY_SCOPE, EMPTY_TYPE_SCHEMA);
                let expr = expr_ctx.plan_expression(&mut context, value)?;
                LogicalQuery2 {
                    root: LogicalOperator::SetVar2(LogicalNode::new(SetVar {
                        name: reference.pop()?, // TODO: Allow compound references?
                        value: expr.try_into_scalar()?,
                    })),
                    scope: Scope::empty(),
                }
            }
            Statement::ShowVariable(ast::ShowVariable { mut reference }) => {
                let name = reference.pop()?; // TODO: Allow compound references?
                let var = self.vars.get_var(&name)?;
                let scope = Scope::with_columns(None, [name.clone()]);
                LogicalQuery2 {
                    root: LogicalOperator::ShowVar2(LogicalNode::new(ShowVar { var: var.clone() })),
                    scope,
                }
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
                LogicalQuery2 {
                    root: LogicalOperator::ResetVar2(LogicalNode::new(ResetVar { var })),
                    scope: Scope::empty(),
                }
            }
            Statement::Attach(attach) => self.plan_attach(attach)?,
            Statement::Detach(detach) => self.plan_detach(detach)?,
            Statement::Describe(describe) => {
                let mut planner = QueryNodePlanner::new(self.bind_data);
                let plan = match describe {
                    ast::Describe::Query(query) => planner.plan_query(&mut context, query)?,
                    ast::Describe::FromNode(from) => planner.plan_from_node(
                        &mut context,
                        from,
                        TypeSchema::empty(),
                        Scope::empty(),
                    )?,
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

                LogicalQuery2 {
                    root: LogicalOperator::Describe2(LogicalNode::new(Describe { schema })),
                    scope: Scope::with_columns(None, ["column_name", "datatype"]),
                }
            }
        };

        Ok((query, context))
    }

    fn plan_attach(&mut self, mut attach: ast::Attach<ResolvedMeta>) -> Result<LogicalQuery2> {
        match attach.attach_type {
            ast::AttachType::Database => {
                let mut options = HashMap::new();
                let planner = QueryNodePlanner::new(self.bind_data);
                let expr_ctx = ExpressionContext::new(&planner, EMPTY_SCOPE, EMPTY_TYPE_SCHEMA);

                for (k, v) in attach.options {
                    let k = k.into_normalized_string();
                    let v = match expr_ctx.plan_expression(&mut QueryContext::new(), v)? {
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

                Ok(LogicalQuery2 {
                    root: LogicalOperator::AttachDatabase2(LogicalNode::new(AttachDatabase {
                        datasource: datasource.into_normalized_string(),
                        name,
                        options,
                    })),
                    scope: Scope::empty(),
                })
            }
            ast::AttachType::Table => Err(RayexecError::new("Attach tables not yet supported")),
        }
    }

    fn plan_detach(&mut self, mut detach: ast::Detach<ResolvedMeta>) -> Result<LogicalQuery2> {
        match detach.attach_type {
            ast::AttachType::Database => {
                if detach.alias.0.len() != 1 {
                    return Err(RayexecError::new(format!(
                        "Expected a single identifier, got '{}'",
                        detach.alias
                    )));
                }
                let name = detach.alias.pop()?;

                Ok(LogicalQuery2 {
                    root: LogicalOperator::DetachDatabase2(LogicalNode::new(DetachDatabase {
                        name,
                    })),
                    scope: Scope::empty(),
                })
            }
            ast::AttachType::Table => Err(RayexecError::new("Detach tables not yet supported")),
        }
    }

    fn plan_copy_to(
        &mut self,
        context: &mut QueryContext,
        copy_to: ast::CopyTo<ResolvedMeta>,
    ) -> Result<LogicalQuery2> {
        let source = match copy_to.source {
            ast::CopyToSource::Query(query) => {
                let mut planner = QueryNodePlanner::new(self.bind_data);
                planner.plan_query(context, query)?
            }
            ast::CopyToSource::Table(table) => {
                let (reference, location) = match self.bind_data.tables.try_get_bound(table)? {
                    (ResolvedTableOrCteReference::Table(reference), location) => {
                        (reference, location)
                    }
                    (ResolvedTableOrCteReference::Cte { .. }, _) => {
                        // Shouldn't be possible.
                        return Err(RayexecError::new("Cannot COPY from CTE"));
                    }
                };

                let scope = Scope::with_columns(
                    None,
                    reference
                        .entry
                        .try_as_table_entry()?
                        .columns
                        .iter()
                        .map(|f| f.name.clone()),
                );

                LogicalQuery2 {
                    root: LogicalOperator::Scan2(LogicalNode::with_location(
                        Scan {
                            catalog: reference.catalog.clone(),
                            schema: reference.schema.clone(),
                            source: reference.entry.clone(),
                        },
                        location,
                    )),
                    scope,
                }
            }
        };

        let source_schema = source.schema()?;
        let bound_copy_to = self
            .bind_data
            .copy_to
            .as_ref()
            .ok_or_else(|| RayexecError::new("Missing COPY TO function"))?
            .clone();

        Ok(LogicalQuery2 {
            root: LogicalOperator::CopyTo2(LogicalNode::with_location(
                CopyTo {
                    source: Box::new(source.root),
                    source_schema,
                    location: copy_to.target,
                    copy_to: bound_copy_to.func,
                },
                LocationRequirement::ClientLocal,
            )),
            scope: Scope::with_columns(None, ["rows_copied"]),
        })
    }

    fn plan_insert(
        &mut self,
        context: &mut QueryContext,
        insert: ast::Insert<ResolvedMeta>,
    ) -> Result<LogicalQuery2> {
        let mut planner = QueryNodePlanner::new(self.bind_data);
        let source = planner.plan_query(context, insert.source)?;

        let (reference, location) = match self.bind_data.tables.try_get_bound(insert.table)? {
            (ResolvedTableOrCteReference::Table(reference), location) => (reference, location),
            (ResolvedTableOrCteReference::Cte { .. }, _) => {
                // Shouldn't be possible.
                return Err(RayexecError::new("Cannot insert into CTE"));
            }
        };

        let table_type_schema = TypeSchema::new(
            reference
                .entry
                .try_as_table_entry()?
                .columns
                .iter()
                .map(|c| c.datatype.clone()),
        );
        let source_schema = source.root.output_schema(&planner.outer_schemas)?;

        let input = Self::apply_cast_for_insert(&table_type_schema, &source_schema, source.root)?;

        // TODO: Handle specified columns. If provided, insert a projection that
        // maps the columns to the right position.

        Ok(LogicalQuery2 {
            root: LogicalOperator::Insert2(LogicalNode::with_location(
                Insert {
                    catalog: reference.catalog.clone(),
                    schema: reference.schema.clone(),
                    table: reference.entry.clone(),
                    input: Box::new(input),
                },
                location,
            )),
            scope: Scope::with_columns(None, ["rows_inserted"]),
        })
    }

    fn plan_drop(&mut self, mut drop: ast::DropStatement<ResolvedMeta>) -> Result<LogicalQuery2> {
        match drop.drop_type {
            ast::DropType::Schema => {
                let [catalog, schema] = drop.name.pop_2()?;

                // Dropping defaults to restricting (erroring) on dependencies.
                let deps = drop.deps.unwrap_or(ast::DropDependents::Restrict);

                let plan = LogicalOperator::Drop2(LogicalNode::new(DropEntry {
                    catalog,
                    info: DropInfo {
                        schema,
                        object: DropObject::Schema,
                        cascade: ast::DropDependents::Cascade == deps,
                        if_exists: drop.if_exists,
                    },
                }));

                Ok(LogicalQuery2 {
                    root: plan,
                    scope: Scope::empty(),
                })
            }
            other => not_implemented!("drop {other:?}"),
        }
    }

    fn plan_create_schema(
        &mut self,
        mut create: ast::CreateSchema<ResolvedMeta>,
    ) -> Result<LogicalQuery2> {
        let on_conflict = if create.if_not_exists {
            OnConflict::Ignore
        } else {
            OnConflict::Error
        };

        let [catalog, schema] = create.name.pop_2()?;

        Ok(LogicalQuery2 {
            root: LogicalOperator::CreateSchema2(LogicalNode::new(CreateSchema {
                catalog,
                name: schema,
                on_conflict,
            })),
            scope: Scope::empty(),
        })
    }

    fn plan_create_table(
        &mut self,
        context: &mut QueryContext,
        mut create: ast::CreateTable<ResolvedMeta>,
    ) -> Result<LogicalQuery2> {
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
            .map(|col| Field::new(col.name.into_normalized_string(), col.datatype, true))
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
                let input = planner.plan_query(context, source)?;
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

        // TODO: Get the location based on the the catalog that we're trying to
        // create a table in. There's still some figuring out how we want to
        // create tables in attached data sources, and if we need to store
        // additional data about the catalog (specifically if the catalog we're
        // referencing is a "stub", and hybrid execution is required).

        Ok(LogicalQuery2 {
            root: LogicalOperator::CreateTable2(LogicalNode::with_location(
                CreateTable {
                    catalog,
                    schema,
                    name,
                    columns,
                    on_conflict,
                    input,
                },
                LocationRequirement::ClientLocal,
            )),
            scope: Scope::empty(),
        })
    }

    /// Applies a projection cast to a root operator for use when inserting into
    /// a table.
    ///
    /// Errors if the number of columns in the plan does not match the number of
    /// types in the schema.
    ///
    /// If no casting is needed, the returned plan will be unchanged.
    fn apply_cast_for_insert(
        cast_to: &TypeSchema,
        root_schema: &TypeSchema,
        root: LogicalOperator,
    ) -> Result<LogicalOperator> {
        // TODO: This will be where we put the projections for default values too.

        if cast_to.types.len() != root_schema.types.len() {
            return Err(RayexecError::new(format!(
                "Invalid number of inputs. Expected {}, got {}",
                cast_to.types.len(),
                root_schema.types.len()
            )));
        }

        let mut projections = Vec::with_capacity(root_schema.types.len());
        let mut num_casts = 0;
        for (col_idx, (want, have)) in cast_to
            .types
            .iter()
            .zip(root_schema.types.iter())
            .enumerate()
        {
            if want == have {
                // No cast needed, just project the column.
                projections.push(LogicalExpression::new_column(col_idx));
            } else {
                // Need to cast.
                projections.push(LogicalExpression::Cast {
                    to: want.clone(),
                    expr: Box::new(LogicalExpression::new_column(col_idx)),
                });
                num_casts += 1;
            }
        }

        if num_casts == 0 {
            // No casting needed, just return the original plan.
            return Ok(root);
        }

        // Otherwise apply projection.
        Ok(LogicalOperator::Projection(LogicalNode::new(Projection {
            exprs: projections,
            input: Box::new(root),
        })))
    }
}