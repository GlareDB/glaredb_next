use rayexec_bullet::datatype::DataType;

use super::Expression;

#[derive(Debug, Clone, PartialEq)]
pub struct CastExpr {
    pub to: DataType,
    pub expr: Box<Expression>,
}
