use rayexec_bullet::scalar::OwnedScalarValue;

#[derive(Debug, Clone, PartialEq)]
pub struct LiteralExpr {
    pub literal: OwnedScalarValue,
}
