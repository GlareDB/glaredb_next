use super::bound_select::BoundSelect;

#[derive(Debug, Clone, PartialEq)]
pub enum BoundQuery {
    Select(BoundSelect),
}
