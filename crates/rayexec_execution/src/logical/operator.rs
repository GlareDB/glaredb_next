use super::binder::bind_context::TableRef;
use super::context::QueryContext;
use super::expr::LogicalExpression;
use super::grouping_set::GroupingSets;
use super::logical_aggregate::LogicalAggregate;
use super::logical_attach::{LogicalAttachDatabase, LogicalDetachDatabase};
use super::logical_copy::LogicalCopyTo;
use super::logical_create::{LogicalCreateSchema, LogicalCreateTable};
use super::logical_describe::LogicalDescribe;
use super::logical_drop::LogicalDrop;
use super::logical_empty::LogicalEmpty;
use super::logical_explain::LogicalExplain;
use super::logical_filter::LogicalFilter;
use super::logical_insert::LogicalInsert;
use super::logical_limit::LogicalLimit;
use super::logical_order::LogicalOrder;
use super::logical_project::LogicalProject;
use super::logical_scan::LogicalScan;
use super::logical_set::{LogicalResetVar, LogicalSetVar, LogicalShowVar};
use crate::database::catalog_entry::CatalogEntry;
use crate::database::create::OnConflict;
use crate::database::drop::DropInfo;
use crate::engine::vars::SessionVar;
use crate::execution::explain::format_logical_plan_for_explain;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::functions::copy::CopyToFunction;
use crate::functions::table::PlannedTableFunction;
use rayexec_bullet::datatype::DataType;
use rayexec_bullet::field::{Field, Schema, TypeSchema};
use rayexec_bullet::scalar::OwnedScalarValue;
use rayexec_error::{not_implemented, RayexecError, Result};
use rayexec_io::location::FileLocation;
use rayexec_proto::ProtoConv;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

pub trait SchemaNode {
    /// Get the output type schema of the operator.
    ///
    /// Since we're working with possibly correlated columns, this also accepts
    /// the schema of the outer scopes.
    ///
    /// During physical planning, it's assumed that the logical plan has been
    /// completely decorrelated, and as such will be provided and empty outer
    /// schema.
    fn output_schema(&self, outer: &[TypeSchema]) -> Result<TypeSchema>;
}

/// Requirement for where a node in the plan needs to be executed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocationRequirement {
    /// Required to be executed locally on the client.
    ClientLocal,
    /// Required to be executed remotely.
    Remote,
    /// Can be executed either locally or remote.
    ///
    /// Unless explicitly required during binding, all nodes should start with
    /// this variant.
    ///
    /// An optimization pass will walk the plan an flip this to either local or
    /// remote depending on where the node sits in the plan.
    Any,
}

impl ProtoConv for LocationRequirement {
    type ProtoType = rayexec_proto::generated::logical::LocationRequirement;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        Ok(match self {
            Self::ClientLocal => Self::ProtoType::ClientLocal,
            Self::Remote => Self::ProtoType::Remote,
            Self::Any => Self::ProtoType::Any,
        })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        Ok(match proto {
            Self::ProtoType::InvalidLocationRequirement => {
                return Err(RayexecError::new("invalid"))
            }
            Self::ProtoType::ClientLocal => Self::ClientLocal,
            Self::ProtoType::Remote => Self::Remote,
            Self::ProtoType::Any => Self::Any,
        })
    }
}

impl fmt::Display for LocationRequirement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ClientLocal => write!(f, "ClientLocal"),
            Self::Remote => write!(f, "Remote"),
            Self::Any => write!(f, "Any"),
        }
    }
}

/// Common operations across all logical nodes in a plan.
///
/// For individual operators, this should be implemented on `Node<T>` and not
/// `T`.
///
/// This is implemented on `LogicalOperator` for convenience.
pub trait LogicalNode {
    /// Returns a list of table refs represent the output of this operator.
    ///
    /// After all planning and optimization, a logical operator should only be
    /// referencing the table refs of its direct children. If this holds, we can
    /// then just generate column indexes when referencing batch columns in
    /// physical operators.
    ///
    /// If a logical operator references a table ref that isn't the output of
    /// any of its immediate children, then we messed up planning (e.g. didn't
    /// fully decorrelate).
    fn get_output_table_refs(&self) -> Vec<TableRef>;
}

/// Wrapper around nodes in the logical plan to holds additional metadata for
/// the node.
#[derive(Debug, Clone, PartialEq)]
pub struct Node<N> {
    /// Node specific logic.
    pub node: N,
    /// Location where this node should be executed.
    ///
    /// May be 'Any' if there's no requirement that this node executes on the
    /// client or server.
    pub location: LocationRequirement,
    /// Inputs to this node.
    pub children: Vec<LogicalOperator>,
}

impl<N> Node<N> {
    /// Create a new logical node without an explicit location requirement.
    // TODO: Remove, should be explicit with everything
    pub const fn new(node: N) -> Self {
        Node {
            node,
            location: LocationRequirement::Any,
            children: Vec::new(),
        }
    }

    /// Create a logical node with a specified location requirement.
    pub fn with_location(node: N, location: LocationRequirement) -> Self {
        Node {
            node,
            location,
            children: Vec::new(),
        }
    }

    pub fn into_inner(self) -> N {
        self.node
    }

    pub fn pop_one_child_exact(&mut self) -> Result<LogicalOperator> {
        if self.children.len() != 1 {
            return Err(RayexecError::new(format!(
                "Expected 1 child to operator, have {}",
                self.children.len()
            )));
        }
        Ok(self.children.pop().unwrap())
    }

    pub fn get_one_child_exact(&self) -> Result<&LogicalOperator> {
        if self.children.len() != 1 {
            return Err(RayexecError::new(format!(
                "Expected 1 child to operator, have {}",
                self.children.len()
            )));
        }
        Ok(&self.children[0])
    }

    /// Get all table refs from the immedidate children of this node.
    pub(crate) fn get_children_table_refs(&self) -> Vec<TableRef> {
        self.children.iter().fold(Vec::new(), |mut refs, child| {
            refs.append(&mut child.get_output_table_refs());
            refs
        })
    }

    pub fn get_table_refs2(&self) -> Vec<TableRef> {
        unimplemented!()
        // if let Some(refs) = self.input_table_refs.clone() {
        //     return refs;
        // }

        // let mut refs = Vec::new();
        // for child in &self.children {
        //     // TODO
        //     unimplemented!()
        // }

        // refs
    }
}

impl<N: Explainable> Explainable for Node<N> {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        let mut ent = self
            .node
            .explain_entry(conf)
            .with_value("location", self.location);

        if conf.verbose {
            ent = ent.with_values("table_refs", self.get_table_refs2());
        }

        ent
    }
}

impl<N> AsRef<N> for Node<N> {
    fn as_ref(&self) -> &N {
        &self.node
    }
}

impl<N> AsMut<N> for Node<N> {
    fn as_mut(&mut self) -> &mut N {
        &mut self.node
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum LogicalOperator {
    Projection(Node<Projection>),
    Filter2(Node<Filter>),
    Aggregate2(Node<Aggregate>),
    Order2(Node<Order>),
    AnyJoin(Node<AnyJoin>),
    EqualityJoin(Node<EqualityJoin>),
    CrossJoin(Node<CrossJoin>),
    DependentJoin(Node<DependentJoin>),
    Limit2(Node<Limit>),
    SetOperation(Node<SetOperation>),
    MaterializedScan(Node<MaterializedScan>),
    Scan2(Node<Scan>),
    TableFunction(Node<TableFunction>),
    ExpressionList(Node<ExpressionList>),
    Empty2(Node<()>),
    SetVar2(Node<SetVar>),
    ShowVar2(Node<ShowVar>),
    ResetVar2(Node<ResetVar>),
    CreateSchema2(Node<CreateSchema>),
    CreateTable2(Node<CreateTable>),
    AttachDatabase2(Node<AttachDatabase>),
    DetachDatabase2(Node<DetachDatabase>),
    Drop2(Node<DropEntry>),
    Insert2(Node<Insert>),
    CopyTo2(Node<CopyTo>),
    Explain2(Node<Explain>),
    Describe2(Node<Describe>),
    // TODO
    Project(Node<LogicalProject>),
    Filter(Node<LogicalFilter>),
    Limit(Node<LogicalLimit>),
    Order(Node<LogicalOrder>),
    Aggregate(Node<LogicalAggregate>),
    Scan(Node<LogicalScan>),
    Empty(Node<LogicalEmpty>),
    SetVar(Node<LogicalSetVar>),
    ResetVar(Node<LogicalResetVar>),
    ShowVar(Node<LogicalShowVar>),
    AttachDatabase(Node<LogicalAttachDatabase>),
    DetachDatabase(Node<LogicalDetachDatabase>),
    Drop(Node<LogicalDrop>),
    Insert(Node<LogicalInsert>),
    CreateSchema(Node<LogicalCreateSchema>),
    CreateTable(Node<LogicalCreateTable>),
    Describe(Node<LogicalDescribe>),
    Explain(Node<LogicalExplain>),
    CopyTo(Node<LogicalCopyTo>),
}

impl LogicalOperator {
    pub(crate) const EMPTY: LogicalOperator = LogicalOperator::Empty2(Node::new(()));

    /// Get the output type schema of the operator.
    ///
    /// Since we're working with possibly correlated columns, this also accepts
    /// the schema of the outer scopes.
    pub fn output_schema(&self, outer: &[TypeSchema]) -> Result<TypeSchema> {
        match self {
            Self::Projection(n) => n.as_ref().output_schema(outer),
            Self::Filter2(n) => n.as_ref().output_schema(outer),
            Self::Aggregate2(n) => n.as_ref().output_schema(outer),
            Self::Order2(n) => n.as_ref().output_schema(outer),
            Self::AnyJoin(n) => n.as_ref().output_schema(outer),
            Self::EqualityJoin(n) => n.as_ref().output_schema(outer),
            Self::CrossJoin(n) => n.as_ref().output_schema(outer),
            Self::DependentJoin(n) => n.as_ref().output_schema(outer),
            Self::Limit2(n) => n.as_ref().output_schema(outer),
            Self::SetOperation(n) => n.as_ref().output_schema(outer),
            Self::MaterializedScan(n) => n.as_ref().output_schema(outer),
            Self::Scan2(n) => n.as_ref().output_schema(outer),
            Self::TableFunction(n) => n.as_ref().output_schema(outer),
            Self::ExpressionList(n) => n.as_ref().output_schema(outer),
            Self::Empty2(_) => Ok(TypeSchema::empty()),
            Self::SetVar2(n) => n.as_ref().output_schema(outer),
            Self::ShowVar2(n) => n.as_ref().output_schema(outer),
            Self::ResetVar2(n) => n.as_ref().output_schema(outer),
            Self::CreateSchema2(n) => n.as_ref().output_schema(outer),
            Self::CreateTable2(n) => n.as_ref().output_schema(outer),
            Self::AttachDatabase2(n) => n.as_ref().output_schema(outer),
            Self::DetachDatabase2(n) => n.as_ref().output_schema(outer),
            Self::Drop2(n) => n.as_ref().output_schema(outer),
            Self::Insert2(n) => n.as_ref().output_schema(outer),
            Self::CopyTo2(n) => n.as_ref().output_schema(outer),
            Self::Explain2(n) => n.as_ref().output_schema(outer),
            Self::Describe2(n) => n.as_ref().output_schema(outer),
            _ => unimplemented!(),
        }
    }

    pub fn location(&self) -> &LocationRequirement {
        match self {
            Self::Projection(n) => &n.location,
            Self::Filter2(n) => &n.location,
            Self::Aggregate2(n) => &n.location,
            Self::Order2(n) => &n.location,
            Self::AnyJoin(n) => &n.location,
            Self::EqualityJoin(n) => &n.location,
            Self::CrossJoin(n) => &n.location,
            Self::DependentJoin(n) => &n.location,
            Self::Limit2(n) => &n.location,
            Self::SetOperation(n) => &n.location,
            Self::MaterializedScan(n) => &n.location,
            Self::Scan2(n) => &n.location,
            Self::TableFunction(n) => &n.location,
            Self::ExpressionList(n) => &n.location,
            Self::Empty2(n) => &n.location,
            Self::SetVar2(n) => &n.location,
            Self::ShowVar2(n) => &n.location,
            Self::ResetVar2(n) => &n.location,
            Self::CreateSchema2(n) => &n.location,
            Self::CreateTable2(n) => &n.location,
            Self::AttachDatabase2(n) => &n.location,
            Self::DetachDatabase2(n) => &n.location,
            Self::Drop2(n) => &n.location,
            Self::Insert2(n) => &n.location,
            Self::CopyTo2(n) => &n.location,
            Self::Explain2(n) => &n.location,
            Self::Describe2(n) => &n.location,
            _ => unimplemented!(),
        }
    }

    pub fn location_mut(&mut self) -> &mut LocationRequirement {
        match self {
            Self::Projection(n) => &mut n.location,
            Self::Filter2(n) => &mut n.location,
            Self::Aggregate2(n) => &mut n.location,
            Self::Order2(n) => &mut n.location,
            Self::AnyJoin(n) => &mut n.location,
            Self::EqualityJoin(n) => &mut n.location,
            Self::CrossJoin(n) => &mut n.location,
            Self::DependentJoin(n) => &mut n.location,
            Self::Limit2(n) => &mut n.location,
            Self::SetOperation(n) => &mut n.location,
            Self::MaterializedScan(n) => &mut n.location,
            Self::Scan2(n) => &mut n.location,
            Self::TableFunction(n) => &mut n.location,
            Self::ExpressionList(n) => &mut n.location,
            Self::Empty2(n) => &mut n.location,
            Self::SetVar2(n) => &mut n.location,
            Self::ShowVar2(n) => &mut n.location,
            Self::ResetVar2(n) => &mut n.location,
            Self::CreateSchema2(n) => &mut n.location,
            Self::CreateTable2(n) => &mut n.location,
            Self::AttachDatabase2(n) => &mut n.location,
            Self::DetachDatabase2(n) => &mut n.location,
            Self::Drop2(n) => &mut n.location,
            Self::Insert2(n) => &mut n.location,
            Self::CopyTo2(n) => &mut n.location,
            Self::Explain2(n) => &mut n.location,
            Self::Describe2(n) => &mut n.location,
            _ => unimplemented!(),
        }
    }

    pub fn take(&mut self) -> Self {
        std::mem::replace(self, Self::EMPTY)
    }

    pub fn take_boxed(self: &mut Box<Self>) -> Box<Self> {
        std::mem::replace(self, Box::new(Self::EMPTY))
    }

    pub fn for_each_child_mut<F>(&mut self, f: &mut F) -> Result<()>
    where
        F: FnMut(&mut LogicalOperator) -> Result<()>,
    {
        match self {
            Self::Projection(n) => f(&mut n.as_mut().input)?,
            Self::Filter2(n) => f(&mut n.as_mut().input)?,
            Self::Aggregate2(n) => f(&mut n.as_mut().input)?,
            Self::Order2(n) => f(&mut n.as_mut().input)?,
            Self::AnyJoin(n) => {
                f(&mut n.as_mut().left)?;
                f(&mut n.as_mut().right)?;
            }
            Self::EqualityJoin(n) => {
                f(&mut n.as_mut().left)?;
                f(&mut n.as_mut().right)?;
            }
            Self::CrossJoin(n) => {
                f(&mut n.as_mut().left)?;
                f(&mut n.as_mut().right)?;
            }
            Self::DependentJoin(n) => {
                f(&mut n.as_mut().left)?;
                f(&mut n.as_mut().right)?;
            }
            Self::Limit2(n) => f(&mut n.as_mut().input)?,
            Self::SetOperation(_) => (),
            Self::MaterializedScan(_) => (),
            Self::Scan2(_) => (),
            Self::TableFunction(_) => (),
            Self::ExpressionList(_) => (),
            Self::Empty2(_) => (),
            Self::SetVar2(_) => (),
            Self::ShowVar2(_) => (),
            Self::ResetVar2(_) => (),
            Self::CreateSchema2(_) => (),
            Self::CreateTable2(_) => (),
            Self::AttachDatabase2(_) => (),
            Self::DetachDatabase2(_) => (),
            Self::Drop2(_) => (),
            Self::Insert2(n) => f(&mut n.as_mut().input)?,
            Self::CopyTo2(n) => f(&mut n.as_mut().source)?,
            Self::Explain2(n) => f(&mut n.as_mut().input)?,
            Self::Describe2(_) => (),
            _ => unimplemented!(),
        }
        Ok(())
    }

    pub fn walk_mut_pre<F>(&mut self, pre: &mut F) -> Result<()>
    where
        F: FnMut(&mut LogicalOperator) -> Result<()>,
    {
        self.walk_mut(pre, &mut |_| Ok(()))
    }

    pub fn walk_mut_post<F>(&mut self, post: &mut F) -> Result<()>
    where
        F: FnMut(&mut LogicalOperator) -> Result<()>,
    {
        self.walk_mut(&mut |_| Ok(()), post)
    }

    /// Walk the plan depth first.
    ///
    /// `pre` provides access to children on the way down, and `post` on the way
    /// up.
    pub fn walk_mut<F1, F2>(&mut self, pre: &mut F1, post: &mut F2) -> Result<()>
    where
        F1: FnMut(&mut LogicalOperator) -> Result<()>,
        F2: FnMut(&mut LogicalOperator) -> Result<()>,
    {
        pre(self)?;
        match self {
            LogicalOperator::Projection(p) => {
                pre(&mut p.as_mut().input)?;
                p.as_mut().input.walk_mut(pre, post)?;
                post(&mut p.as_mut().input)?;
            }
            LogicalOperator::Filter2(p) => {
                pre(&mut p.as_mut().input)?;
                p.as_mut().input.walk_mut(pre, post)?;
                post(&mut p.as_mut().input)?;
            }
            LogicalOperator::Aggregate2(p) => {
                pre(&mut p.as_mut().input)?;
                p.as_mut().input.walk_mut(pre, post)?;
                post(&mut p.as_mut().input)?;
            }
            LogicalOperator::Order2(p) => {
                pre(&mut p.as_mut().input)?;
                p.as_mut().input.walk_mut(pre, post)?;
                post(&mut p.as_mut().input)?;
            }
            LogicalOperator::Limit2(p) => {
                pre(&mut p.as_mut().input)?;
                p.as_mut().input.walk_mut(pre, post)?;
                post(&mut p.as_mut().input)?;
            }
            LogicalOperator::CrossJoin(p) => {
                pre(&mut p.as_mut().left)?;
                p.as_mut().left.walk_mut(pre, post)?;
                post(&mut p.as_mut().left)?;

                pre(&mut p.as_mut().right)?;
                p.as_mut().right.walk_mut(pre, post)?;
                post(&mut p.as_mut().right)?;
            }
            LogicalOperator::DependentJoin(p) => {
                pre(&mut p.as_mut().left)?;
                p.as_mut().left.walk_mut(pre, post)?;
                post(&mut p.as_mut().left)?;

                pre(&mut p.as_mut().right)?;
                p.as_mut().right.walk_mut(pre, post)?;
                post(&mut p.as_mut().right)?;
            }
            LogicalOperator::AnyJoin(p) => {
                pre(&mut p.as_mut().left)?;
                p.as_mut().left.walk_mut(pre, post)?;
                post(&mut p.as_mut().left)?;

                pre(&mut p.as_mut().right)?;
                p.as_mut().right.walk_mut(pre, post)?;
                post(&mut p.as_mut().right)?;
            }
            LogicalOperator::EqualityJoin(p) => {
                pre(&mut p.as_mut().left)?;
                p.as_mut().left.walk_mut(pre, post)?;
                post(&mut p.as_mut().left)?;

                pre(&mut p.as_mut().right)?;
                p.as_mut().right.walk_mut(pre, post)?;
                post(&mut p.as_mut().right)?;
            }
            LogicalOperator::SetOperation(p) => {
                pre(&mut p.as_mut().top)?;
                p.as_mut().top.walk_mut(pre, post)?;
                post(&mut p.as_mut().top)?;

                pre(&mut p.as_mut().bottom)?;
                p.as_mut().bottom.walk_mut(pre, post)?;
                post(&mut p.as_mut().bottom)?;
            }
            LogicalOperator::Insert2(p) => {
                pre(&mut p.as_mut().input)?;
                p.as_mut().input.walk_mut(pre, post)?;
                post(&mut p.as_mut().input)?;
            }
            LogicalOperator::Explain2(p) => {
                pre(&mut p.as_mut().input)?;
                p.as_mut().input.walk_mut(pre, post)?;
                post(&mut p.as_mut().input)?;
            }
            LogicalOperator::CopyTo2(p) => {
                pre(&mut p.as_mut().source)?;
                p.as_mut().source.walk_mut(pre, post)?;
                post(&mut p.as_mut().source)?;
            }
            LogicalOperator::ExpressionList(_)
            | LogicalOperator::Empty2(_)
            | LogicalOperator::SetVar2(_)
            | LogicalOperator::ShowVar2(_)
            | LogicalOperator::ResetVar2(_)
            | LogicalOperator::CreateTable2(_)
            | LogicalOperator::CreateSchema2(_)
            | LogicalOperator::AttachDatabase2(_)
            | LogicalOperator::DetachDatabase2(_)
            | LogicalOperator::Drop2(_)
            | LogicalOperator::MaterializedScan(_)
            | LogicalOperator::Scan2(_)
            | LogicalOperator::Describe2(_)
            | LogicalOperator::TableFunction(_) => (),
            _ => unimplemented!(),
        }
        post(self)?;

        Ok(())
    }

    /// Return the explain string for a plan. Useful for println debugging.
    #[allow(dead_code)]
    pub(crate) fn debug_explain(&self, context: Option<&QueryContext>) -> String {
        format_logical_plan_for_explain(context, self, ExplainFormat::Text, true).unwrap()
    }
}

impl LogicalNode for LogicalOperator {
    fn get_output_table_refs(&self) -> Vec<TableRef> {
        unimplemented!()
    }
}

impl Explainable for LogicalOperator {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        match self {
            Self::Projection(p) => p.as_ref().explain_entry(conf),
            Self::Filter2(p) => p.as_ref().explain_entry(conf),
            Self::Aggregate2(p) => p.as_ref().explain_entry(conf),
            Self::Order2(p) => p.as_ref().explain_entry(conf),
            Self::AnyJoin(p) => p.as_ref().explain_entry(conf),
            Self::EqualityJoin(p) => p.as_ref().explain_entry(conf),
            Self::CrossJoin(p) => p.as_ref().explain_entry(conf),
            Self::DependentJoin(p) => p.as_ref().explain_entry(conf),
            Self::Limit2(p) => p.as_ref().explain_entry(conf),
            Self::SetOperation(p) => p.as_ref().explain_entry(conf),
            Self::MaterializedScan(p) => p.as_ref().explain_entry(conf),
            Self::Scan2(p) => p.as_ref().explain_entry(conf),
            Self::TableFunction(p) => p.as_ref().explain_entry(conf),
            Self::ExpressionList(p) => p.as_ref().explain_entry(conf),
            Self::SetVar2(p) => p.as_ref().explain_entry(conf),
            Self::ShowVar2(p) => p.as_ref().explain_entry(conf),
            Self::ResetVar2(p) => p.as_ref().explain_entry(conf),
            Self::CreateSchema2(p) => p.as_ref().explain_entry(conf),
            Self::CreateTable2(p) => p.as_ref().explain_entry(conf),
            Self::AttachDatabase2(n) => n.as_ref().explain_entry(conf),
            Self::DetachDatabase2(n) => n.as_ref().explain_entry(conf),
            Self::Drop2(p) => p.as_ref().explain_entry(conf),
            Self::Insert2(p) => p.as_ref().explain_entry(conf),
            Self::Explain2(p) => p.as_ref().explain_entry(conf),
            Self::CopyTo2(p) => p.as_ref().explain_entry(conf),
            Self::Describe2(p) => p.as_ref().explain_entry(conf),
            _ => unimplemented!(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Projection {
    pub exprs: Vec<LogicalExpression>,
    pub input: Box<LogicalOperator>,
}

impl SchemaNode for Projection {
    fn output_schema(&self, outer: &[TypeSchema]) -> Result<TypeSchema> {
        let current = self.input.output_schema(outer)?;
        let types = self
            .exprs
            .iter()
            .map(|expr| expr.datatype(&current, outer))
            .collect::<Result<Vec<_>>>()?;
        Ok(TypeSchema::new(types))
    }
}

impl Explainable for Projection {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Projection").with_values(
            "expressions",
            self.exprs.iter().map(|expr| format!("{expr}")),
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Filter {
    pub predicate: LogicalExpression,
    pub input: Box<LogicalOperator>,
}

impl SchemaNode for Filter {
    fn output_schema(&self, outer: &[TypeSchema]) -> Result<TypeSchema> {
        // Filter just filters out rows, no column changes happen.
        self.input.output_schema(outer)
    }
}

impl Explainable for Filter {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Filter").with_value("predicate", format!("{}", self.predicate))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderByExpr {
    pub expr: LogicalExpression,
    pub desc: bool,
    pub nulls_first: bool,
}

impl fmt::Display for OrderByExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {} {}",
            self.expr,
            if self.desc { "DESC" } else { "ASC" },
            if self.nulls_first {
                "NULLS FIRST"
            } else {
                "NULLS LAST"
            }
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Order {
    pub exprs: Vec<OrderByExpr>,
    pub input: Box<LogicalOperator>,
}

impl SchemaNode for Order {
    fn output_schema(&self, outer: &[TypeSchema]) -> Result<TypeSchema> {
        // Order by doesn't change row output.
        self.input.output_schema(outer)
    }
}

impl Explainable for Order {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Order").with_values(
            "expressions",
            self.exprs.iter().map(|expr| format!("{expr}")),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Left,
    Right,
    Inner,
    Full,
    Semi,
    Anti,
    Cross,
}

impl fmt::Display for JoinType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Inner => write!(f, "INNER"),
            Self::Left => write!(f, "LEFT"),
            Self::Right => write!(f, "RIGHT"),
            Self::Full => write!(f, "FULL"),
            Self::Semi => write!(f, "SEMI"),
            Self::Anti => write!(f, "ANTI"),
            Self::Cross => write!(f, "CROSS"),
        }
    }
}

/// A join on an arbitrary expression.
#[derive(Debug, PartialEq, Clone)]
pub struct AnyJoin {
    pub left: Box<LogicalOperator>,
    pub right: Box<LogicalOperator>,
    pub join_type: JoinType,
    pub on: LogicalExpression,
}

impl SchemaNode for AnyJoin {
    fn output_schema(&self, outer: &[TypeSchema]) -> Result<TypeSchema> {
        let left = self.left.output_schema(outer)?;
        let right = self.right.output_schema(outer)?;
        Ok(left.merge(right))
    }
}

impl Explainable for AnyJoin {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("AnyJoin")
            .with_value("join", self.join_type)
            .with_value("on", &self.on)
    }
}

/// A join on left/right column equality.
#[derive(Debug, Clone, PartialEq)]
pub struct EqualityJoin {
    pub left: Box<LogicalOperator>,
    pub right: Box<LogicalOperator>,
    pub join_type: JoinType,
    pub left_on: Vec<usize>,
    pub right_on: Vec<usize>,
    // TODO: Filter
    // TODO: NULL == NULL
}

impl SchemaNode for EqualityJoin {
    fn output_schema(&self, outer: &[TypeSchema]) -> Result<TypeSchema> {
        let left = self.left.output_schema(outer)?;
        let right = self.right.output_schema(outer)?;
        Ok(left.merge(right))
    }
}

impl Explainable for EqualityJoin {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("EqualityJoin").with_value("join", self.join_type)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CrossJoin {
    pub left: Box<LogicalOperator>,
    pub right: Box<LogicalOperator>,
}

impl SchemaNode for CrossJoin {
    fn output_schema(&self, outer: &[TypeSchema]) -> Result<TypeSchema> {
        let left = self.left.output_schema(outer)?;
        let right = self.right.output_schema(outer)?;
        Ok(left.merge(right))
    }
}

impl Explainable for CrossJoin {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("CrossJoin")
    }
}

/// A join where the right input has columns that depend on output in the left.
#[derive(Debug, Clone, PartialEq)]
pub struct DependentJoin {
    pub left: Box<LogicalOperator>,
    pub right: Box<LogicalOperator>,
}

impl SchemaNode for DependentJoin {
    fn output_schema(&self, outer: &[TypeSchema]) -> Result<TypeSchema> {
        let left = self.left.output_schema(outer)?;
        let right = self.right.output_schema(outer)?;
        Ok(left.merge(right))
    }
}

impl Explainable for DependentJoin {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("DependentJoin")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Limit {
    pub offset: Option<usize>,
    pub limit: usize,
    pub input: Box<LogicalOperator>,
}

impl SchemaNode for Limit {
    fn output_schema(&self, outer: &[TypeSchema]) -> Result<TypeSchema> {
        self.input.output_schema(outer)
    }
}

impl Explainable for Limit {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Limit")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOpKind {
    Union,
    Except,
    Intersect,
}

impl fmt::Display for SetOpKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Union => write!(f, "UNION"),
            Self::Except => write!(f, "EXCEPT"),
            Self::Intersect => write!(f, "INTERSECT"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SetOperation {
    pub top: Box<LogicalOperator>,
    pub bottom: Box<LogicalOperator>,
    pub kind: SetOpKind,
    pub all: bool,
}

impl SchemaNode for SetOperation {
    fn output_schema(&self, outer: &[TypeSchema]) -> Result<TypeSchema> {
        self.top.output_schema(outer)
    }
}

impl Explainable for SetOperation {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("SetOperation").with_value(
            "operation",
            if self.all {
                format!("{} ALL", self.kind)
            } else {
                self.kind.to_string()
            },
        )
    }
}

/// A scan of a materialized plan.
#[derive(Debug, Clone, PartialEq)]
pub struct MaterializedScan {
    /// Index of the materialized plan in the query context.
    pub idx: usize,

    /// Compute type schema of the underlying materialized plan.
    // TODO: This currently exists on the operator to avoid needing to pass the
    // QueryContext into `output_schema`. I actually think storing the schema is
    // preferred to what we're currently doing where we compute it every time,
    // but everything else needs to change in order to make it all consistent.
    //
    // I also believe that we can remove the `outer` stuff with my current plan
    // for subqueries, but that's stil tbd.
    pub schema: TypeSchema,
}

impl SchemaNode for MaterializedScan {
    fn output_schema(&self, _outer: &[TypeSchema]) -> Result<TypeSchema> {
        Ok(self.schema.clone())
    }
}

impl Explainable for MaterializedScan {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("MaterializedScan")
            .with_value("idx", self.idx)
            .with_values("column_types", &self.schema.types)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CopyTo {
    pub source: Box<LogicalOperator>,
    /// Schema of input operator.
    ///
    /// Stored on this operator since the copy to sinks may need field names
    /// (e.g. writing out a header in csv).
    pub source_schema: Schema,
    pub location: FileLocation,
    pub copy_to: Box<dyn CopyToFunction>,
}

impl SchemaNode for CopyTo {
    fn output_schema(&self, _outer: &[TypeSchema]) -> Result<TypeSchema> {
        // Number of rows returned
        Ok(TypeSchema::new([DataType::UInt64]))
    }
}

impl Explainable for CopyTo {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("CopyTo").with_value("location", &self.location)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Scan {
    pub catalog: String,
    pub schema: String,
    pub source: Arc<CatalogEntry>,
    // pub projection: Option<Vec<usize>>,
    // pub input: BindIdx,
    // TODO: Pushdowns
}

impl SchemaNode for Scan {
    fn output_schema(&self, _outer: &[TypeSchema]) -> Result<TypeSchema> {
        let schema = TypeSchema::new(
            self.source
                .try_as_table_entry()?
                .columns
                .iter()
                .map(|f| f.datatype.clone()),
        );
        Ok(schema)
    }
}

impl Explainable for Scan {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        let column_types = self
            .source
            .try_as_table_entry()
            .expect("entry to be a table entry")
            .columns
            .iter()
            .map(|c| c.datatype.clone());
        ExplainEntry::new("Scan")
            .with_value("source", &self.source.name)
            .with_values("column_types", column_types)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableFunction {
    pub function: Box<dyn PlannedTableFunction>,
}

impl SchemaNode for TableFunction {
    fn output_schema(&self, _outer: &[TypeSchema]) -> Result<TypeSchema> {
        Ok(self.function.schema().type_schema())
    }
}

impl Explainable for TableFunction {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("TableFunction")
            .with_value("function", self.function.table_function().name())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExpressionList {
    pub rows: Vec<Vec<LogicalExpression>>,
    // TODO: Table index. What?
}

impl SchemaNode for ExpressionList {
    fn output_schema(&self, outer: &[TypeSchema]) -> Result<TypeSchema> {
        let first = self
            .rows
            .first()
            .ok_or_else(|| RayexecError::new("Expression list contains no rows"))?;
        // No inputs to expression list. Attempting to reference a
        // column should error.
        let current = TypeSchema::empty();
        let types = first
            .iter()
            .map(|expr| expr.datatype(&current, outer))
            .collect::<Result<Vec<_>>>()?;
        Ok(TypeSchema::new(types))
    }
}

impl Explainable for ExpressionList {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        let mut ent = ExplainEntry::new("ExpressionList");
        for (idx, row) in self.rows.iter().enumerate() {
            ent = ent.with_values(format!("row{idx}"), row);
        }
        ent
    }
}

/// An aggregate node containing some number of aggregates, and optional groups.
///
/// The output schema of the this node is [aggregate_columns, group_columns]. A
/// projection above this node should be used to reorder the columns as needed.
#[derive(Debug, Clone, PartialEq)]
pub struct Aggregate {
    /// Aggregate functions calls.
    ///
    /// During planning, the aggregate function calls will be replaced with
    /// column references.
    pub aggregates: Vec<LogicalExpression>,

    /// Expressions in the GROUP BY clauses.
    ///
    /// Empty indicates we'll be computing an aggregate over a single group.
    pub group_exprs: Vec<LogicalExpression>,

    /// Optional grouping set.
    pub grouping_sets: Option<GroupingSets>,

    /// Input to the aggregate.
    pub input: Box<LogicalOperator>,
}

impl SchemaNode for Aggregate {
    fn output_schema(&self, outer: &[TypeSchema]) -> Result<TypeSchema> {
        let current = self.input.output_schema(outer)?;
        let types = self
            .aggregates
            .iter()
            .chain(self.group_exprs.iter())
            .map(|expr| expr.datatype(&current, outer))
            .collect::<Result<Vec<_>>>()?;

        Ok(TypeSchema::new(types))
    }
}

impl Explainable for Aggregate {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        let mut ent = ExplainEntry::new("Aggregate")
            .with_values("aggregates", &self.aggregates)
            .with_values("group_exprs", &self.group_exprs);

        if let Some(grouping_set) = &self.grouping_sets {
            ent = ent.with_value("grouping_sets", grouping_set.num_groups());
        }

        ent
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateSchema {
    pub catalog: String,
    pub name: String,
    pub on_conflict: OnConflict,
}

impl SchemaNode for CreateSchema {
    fn output_schema(&self, _outer: &[TypeSchema]) -> Result<TypeSchema> {
        Ok(TypeSchema::empty())
    }
}

impl Explainable for CreateSchema {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("CreateSchema").with_value("name", &self.name)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTable {
    pub catalog: String,
    pub schema: String,
    pub name: String,
    pub columns: Vec<Field>,
    pub on_conflict: OnConflict,
    /// Optional input for CREATE TABLE AS
    pub input: Option<Box<LogicalOperator>>,
}

impl SchemaNode for CreateTable {
    fn output_schema(&self, _outer: &[TypeSchema]) -> Result<TypeSchema> {
        Ok(TypeSchema::empty())
    }
}

impl Explainable for CreateTable {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("CreateTable")
            .with_values("columns", self.columns.iter().map(|c| &c.name))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AttachDatabase {
    pub datasource: String,
    pub name: String,
    pub options: HashMap<String, OwnedScalarValue>,
}

impl SchemaNode for AttachDatabase {
    fn output_schema(&self, _outer: &[TypeSchema]) -> Result<TypeSchema> {
        Ok(TypeSchema::empty())
    }
}

impl Explainable for AttachDatabase {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("AttachDatabase")
            .with_value("datasource", &self.datasource)
            .with_value("name", &self.name)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DetachDatabase {
    pub name: String,
}

impl SchemaNode for DetachDatabase {
    fn output_schema(&self, _outer: &[TypeSchema]) -> Result<TypeSchema> {
        Ok(TypeSchema::empty())
    }
}

impl Explainable for DetachDatabase {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("DetachDatabase").with_value("name", &self.name)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropEntry {
    pub catalog: String,
    pub info: DropInfo,
}

impl SchemaNode for DropEntry {
    fn output_schema(&self, _outer: &[TypeSchema]) -> Result<TypeSchema> {
        Ok(TypeSchema::empty())
    }
}

impl Explainable for DropEntry {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Drop")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Insert {
    pub catalog: String,
    pub schema: String,
    pub table: Arc<CatalogEntry>,
    pub input: Box<LogicalOperator>,
}

impl SchemaNode for Insert {
    fn output_schema(&self, _outer: &[TypeSchema]) -> Result<TypeSchema> {
        // TODO: RETURNING would chang this.

        // Number of rows returned.
        Ok(TypeSchema::new([DataType::UInt64]))
    }
}

impl Explainable for Insert {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Insert")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SetVar {
    pub name: String,
    pub value: OwnedScalarValue,
}

impl SchemaNode for SetVar {
    fn output_schema(&self, _outer: &[TypeSchema]) -> Result<TypeSchema> {
        Ok(TypeSchema::empty())
    }
}

impl Explainable for SetVar {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("SetVar")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ShowVar {
    pub var: SessionVar,
}

impl SchemaNode for ShowVar {
    fn output_schema(&self, _outer: &[TypeSchema]) -> Result<TypeSchema> {
        // TODO: Double check with postgres if they convert everything to
        // string in SHOW. I'm adding this in right now just to quickly
        // check the vars for debugging.
        Ok(TypeSchema::new(vec![DataType::Utf8]))
    }
}

impl Explainable for ShowVar {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("ShowVar")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum VariableOrAll {
    Variable(SessionVar),
    All,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResetVar {
    pub var: VariableOrAll,
}

impl SchemaNode for ResetVar {
    fn output_schema(&self, _outer: &[TypeSchema]) -> Result<TypeSchema> {
        Ok(TypeSchema::empty())
    }
}

impl Explainable for ResetVar {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("ResetVar")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExplainFormat {
    Text,
    Json,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Explain {
    pub analyze: bool,
    pub verbose: bool,
    pub format: ExplainFormat,
    pub input: Box<LogicalOperator>,
}

impl SchemaNode for Explain {
    fn output_schema(&self, _outer: &[TypeSchema]) -> Result<TypeSchema> {
        Ok(TypeSchema::new(vec![DataType::Utf8, DataType::Utf8]))
    }
}

impl Explainable for Explain {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Explain")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Describe {
    pub schema: Schema,
}

impl SchemaNode for Describe {
    fn output_schema(&self, _outer: &[TypeSchema]) -> Result<TypeSchema> {
        Ok(TypeSchema::new(vec![DataType::Utf8, DataType::Utf8]))
    }
}

impl Explainable for Describe {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Describe")
    }
}

#[cfg(test)]
mod tests {
    use rayexec_bullet::scalar::OwnedScalarValue;

    use super::*;

    #[test]
    fn walk_plan_pre_post() {
        let mut plan = LogicalOperator::Projection(Node::new(Projection {
            exprs: Vec::new(),
            input: Box::new(LogicalOperator::Filter2(Node::new(Filter {
                predicate: LogicalExpression::Literal(OwnedScalarValue::Null),
                input: Box::new(LogicalOperator::Empty2(Node::new(()))),
            }))),
        }));

        plan.walk_mut(
            &mut |child| {
                match child {
                    LogicalOperator::Projection(proj) => proj
                        .as_mut()
                        .exprs
                        .push(LogicalExpression::Literal(OwnedScalarValue::Int8(1))),
                    LogicalOperator::Filter2(_) => {}
                    LogicalOperator::Empty2(_) => {}
                    other => panic!("unexpected child {other:?}"),
                }
                Ok(())
            },
            &mut |child| {
                match child {
                    LogicalOperator::Projection(proj) => {
                        assert_eq!(
                            vec![LogicalExpression::Literal(OwnedScalarValue::Int8(1))],
                            proj.as_ref().exprs
                        );
                        proj.as_mut()
                            .exprs
                            .push(LogicalExpression::Literal(OwnedScalarValue::Int8(2)))
                    }
                    LogicalOperator::Filter2(_) => {}
                    LogicalOperator::Empty2(_) => {}
                    other => panic!("unexpected child {other:?}"),
                }
                Ok(())
            },
        )
        .unwrap();

        match plan {
            LogicalOperator::Projection(proj) => {
                assert_eq!(
                    vec![
                        LogicalExpression::Literal(OwnedScalarValue::Int8(1)),
                        LogicalExpression::Literal(OwnedScalarValue::Int8(2)),
                    ],
                    proj.as_ref().exprs
                );
            }
            other => panic!("unexpected root {other:?}"),
        }
    }
}
