use super::expr_binder::ExpressionBinder;

#[derive(Debug)]
pub struct SelectExprBinder<'a> {
    /// Base expression binder.
    binder: ExpressionBinder<'a>,
}

impl<'a> SelectExprBinder<'a> {}
