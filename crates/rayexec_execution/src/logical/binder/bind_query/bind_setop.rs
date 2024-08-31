use rayexec_error::{not_implemented, RayexecError, Result};
use rayexec_parser::ast;

use crate::{
    functions::implicit::implicit_cast_score,
    logical::{
        binder::{
            bind_context::{BindContext, BindScopeRef, TableRef},
            bind_query::{bind_modifier::ModifierBinder, select_list::SelectList, QueryBinder},
        },
        operator::SetOpKind,
        resolver::{resolve_context::ResolveContext, ResolvedMeta},
    },
};

use super::{
    bind_modifier::{BoundLimit, BoundOrderBy},
    BoundQuery,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOpCastRequirement {
    /// Need to cast the left side to match expected types.
    LeftNeedsCast,
    /// Need to cast the right side to match expected types.
    RightNeedsCast,
    /// Both sides need casting.
    BothNeedsCast,
    /// No sides need casting.
    None,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundSetOp {
    pub left: BoundQuery,
    pub left_scope: BindScopeRef,
    pub right: BoundQuery,
    pub right_scope: BindScopeRef,
    pub setop_table: TableRef,
    pub kind: SetOpKind,
    pub all: bool,
    /// Bound ORDER BY.
    pub order_by: Option<BoundOrderBy>,
    /// Bound LIMIT.
    pub limit: Option<BoundLimit>,
    pub cast_req: SetOpCastRequirement,
}

#[derive(Debug)]
pub struct SetOpBinder<'a> {
    pub current: BindScopeRef,
    pub resolve_context: &'a ResolveContext,
}

impl<'a> SetOpBinder<'a> {
    pub fn new(current: BindScopeRef, resolve_context: &'a ResolveContext) -> Self {
        SetOpBinder {
            current,
            resolve_context,
        }
    }

    pub fn bind(
        &self,
        bind_context: &mut BindContext,
        setop: ast::SetOp<ResolvedMeta>,
        order_by: Option<ast::OrderByModifier<ResolvedMeta>>,
        limit: ast::LimitModifier<ResolvedMeta>,
    ) -> Result<BoundSetOp> {
        let left_scope = bind_context.new_child_scope(self.current);
        // TODO: Make limit modifier optional.
        let left = QueryBinder::new(left_scope, self.resolve_context).bind_body(
            bind_context,
            *setop.left,
            None,
            ast::LimitModifier {
                limit: None,
                offset: None,
            },
        )?;

        let right_scope = bind_context.new_child_scope(self.current);
        let right = QueryBinder::new(left_scope, self.resolve_context).bind_body(
            bind_context,
            *setop.right,
            None,
            ast::LimitModifier {
                limit: None,
                offset: None,
            },
        )?;

        let mut left_types = Vec::new();
        let mut left_names = Vec::new();
        for table in bind_context.iter_tables(left_scope)? {
            left_types.extend_from_slice(&table.column_types);
            left_names.extend_from_slice(&table.column_names);
        }

        let right_types: Vec<_> = bind_context
            .iter_tables(right_scope)?
            .flat_map(|t| t.column_types.iter().cloned())
            .collect();

        // Determine output types of this node by comparing both sides, and
        // marking which side neds casting.
        let mut output_types = Vec::with_capacity(left_types.len());
        let mut left_needs_cast = false;
        let mut right_needs_cast = false;

        for (left, right) in left_types.into_iter().zip(right_types) {
            if left == right {
                // Nothing to do.
                output_types.push(left);
                continue;
            }

            let left_score = implicit_cast_score(&right, left.datatype_id());
            let right_score = implicit_cast_score(&left, right.datatype_id());

            if left_score == 0 && right_score == 0 {
                return Err(RayexecError::new(format!(
                    "Cannot find suitable cast type for {left} and {right}"
                )));
            }

            if left_score >= right_score {
                output_types.push(left);
                right_needs_cast = true;
            } else {
                output_types.push(right);
                left_needs_cast = true;
            }
        }

        let cast_req = match (left_needs_cast, right_needs_cast) {
            (true, true) => SetOpCastRequirement::BothNeedsCast,
            (true, false) => SetOpCastRequirement::LeftNeedsCast,
            (false, true) => SetOpCastRequirement::RightNeedsCast,
            (false, false) => SetOpCastRequirement::None,
        };

        // Move output into scope.
        let table_ref = bind_context.push_table(self.current, None, output_types, left_names)?;

        // ORDER BY and LIMIT on output of the setop.
        let modifier_binder = ModifierBinder::new(vec![self.current], self.resolve_context);
        // TODO: This select list should be able to reference aliases in the output.
        let mut empty_select_list = SelectList::try_new(bind_context, Vec::new())?;
        let order_by = order_by
            .map(|order_by| {
                modifier_binder.bind_order_by(bind_context, &mut empty_select_list, order_by)
            })
            .transpose()?;
        let limit = modifier_binder.bind_limit(bind_context, limit)?;

        if !empty_select_list.appended.is_empty() {
            // Only support ordering by columns, no expressions beyond that yet.
            not_implemented!("ORDER BY expressions");
        }

        let kind = match setop.operation {
            ast::SetOperation::Union => SetOpKind::Union,
            ast::SetOperation::Except => SetOpKind::Except,
            ast::SetOperation::Intersect => SetOpKind::Intersect,
        };

        Ok(BoundSetOp {
            left,
            left_scope,
            right,
            right_scope,
            setop_table: table_ref,
            kind,
            all: setop.all,
            order_by,
            limit,
            cast_req,
        })
    }
}
