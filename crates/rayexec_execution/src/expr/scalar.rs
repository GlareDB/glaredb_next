use rayexec_bullet::array::Array;
use rayexec_bullet::compute::{arith, cmp};
use rayexec_bullet::field::DataType;
use rayexec_error::{RayexecError, Result};
use rayexec_parser::ast;
use std::fmt;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum UnaryOperator {
    IsTrue,
    IsFalse,
    IsNull,
    IsNotNull,
    Negate,
    Cast { to: DataType },
}

impl fmt::Display for UnaryOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IsTrue => write!(f, "IS TRUE"),
            Self::IsFalse => write!(f, "IS FALSE"),
            Self::IsNull => write!(f, "IS NULL"),
            Self::IsNotNull => write!(f, "IS NOT NULL"),
            Self::Negate => write!(f, "-"),
            Self::Cast { to } => write!(f, "CAST TO {to}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BinaryOperator {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,
    And,
    Or,
}

impl fmt::Display for BinaryOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Eq => write!(f, "="),
            Self::NotEq => write!(f, "!="),
            Self::Lt => write!(f, "<"),
            Self::LtEq => write!(f, "<="),
            Self::Gt => write!(f, ">"),
            Self::GtEq => write!(f, ">="),
            Self::Plus => write!(f, "+"),
            Self::Minus => write!(f, "-"),
            Self::Multiply => write!(f, "*"),
            Self::Divide => write!(f, "/"),
            Self::Modulo => write!(f, "%"),
            Self::And => write!(f, "AND"),
            Self::Or => write!(f, "OR"),
        }
    }
}

impl BinaryOperator {
    pub fn data_type(&self, left: &DataType, right: &DataType) -> Result<DataType> {
        use BinaryOperator::*;

        Ok(match self {
            Eq | NotEq | Lt | LtEq | Gt | GtEq | And | Or => DataType::Boolean,
            _ => unimplemented!(), // Plus | Minus | Multiply | Divide | Modulo => maybe_widen(left, right).ok_or_else(|| RayexecError::new(format!("Unable to determine output data type for {:?} using arguments {:?} and {:?}", self, left, right)))?
        })
    }

    pub fn eval(&self, left: &Array, right: &Array) -> Result<Array> {
        let arr = match self {
            BinaryOperator::Eq => Array::Boolean(cmp::eq(&left, &right)?),
            BinaryOperator::NotEq => Array::Boolean(cmp::neq(&left, &right)?),
            BinaryOperator::Lt => Array::Boolean(cmp::lt(&left, &right)?),
            BinaryOperator::LtEq => Array::Boolean(cmp::lt_eq(&left, &right)?),
            BinaryOperator::Gt => Array::Boolean(cmp::gt(&left, &right)?),
            BinaryOperator::GtEq => Array::Boolean(cmp::gt_eq(&left, &right)?),
            // BinaryOperator::Plus => arith::add(&left, &right)?,
            // BinaryOperator::Minus => arith::sub(&left, &right)?,
            // BinaryOperator::Multiply => arith::mul(&left, &right)?,
            // BinaryOperator::Divide => arith::div(&left, &right)?,
            // BinaryOperator::Modulo => arith::rem(&left, &right)?,
            _ => unimplemented!(),
        };

        Ok(arr)
    }
}

impl TryFrom<ast::BinaryOperator> for BinaryOperator {
    type Error = RayexecError;
    fn try_from(value: ast::BinaryOperator) -> Result<Self> {
        Ok(match value {
            ast::BinaryOperator::Plus => BinaryOperator::Plus,
            ast::BinaryOperator::Minus => BinaryOperator::Minus,
            ast::BinaryOperator::Multiply => BinaryOperator::Multiply,
            ast::BinaryOperator::Divide => BinaryOperator::Divide,
            ast::BinaryOperator::Modulo => BinaryOperator::Modulo,
            ast::BinaryOperator::Eq => BinaryOperator::Eq,
            ast::BinaryOperator::NotEq => BinaryOperator::NotEq,
            ast::BinaryOperator::Gt => BinaryOperator::Gt,
            ast::BinaryOperator::GtEq => BinaryOperator::GtEq,
            ast::BinaryOperator::Lt => BinaryOperator::Lt,
            ast::BinaryOperator::LtEq => BinaryOperator::LtEq,
            ast::BinaryOperator::And => BinaryOperator::And,
            ast::BinaryOperator::Or => BinaryOperator::Or,
            other => {
                return Err(RayexecError::new(format!(
                    "Unsupported SQL operator: {other:?}"
                )))
            }
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum VariadicOperator {
    And,
    Or,
}

impl fmt::Display for VariadicOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::And => write!(f, "AND"),
            Self::Or => write!(f, "OR"),
        }
    }
}
