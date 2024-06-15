pub mod arith;
pub mod boolean;
pub mod comparison;
pub mod negate;
pub mod numeric;
pub mod string;
pub mod struct_funcs;

use dyn_clone::DynClone;
use once_cell::sync::Lazy;
use rayexec_bullet::{array::Array, field::DataType};
use rayexec_error::{RayexecError, Result};
use std::fmt::Debug;
use std::sync::Arc;

use super::{FunctionInfo, ReturnType, Signature};

// List of all scalar functions.
pub static BUILTIN_SCALAR_FUNCTIONS: Lazy<Vec<Box<dyn GenericScalarFunction>>> = Lazy::new(|| {
    vec![
        // Arith
        Box::new(arith::Add),
        Box::new(arith::Sub),
        Box::new(arith::Mul),
        Box::new(arith::Div),
        Box::new(arith::Rem),
        // Boolean
        Box::new(boolean::And),
        Box::new(boolean::Or),
        // Comparison
        Box::new(comparison::Eq),
        Box::new(comparison::Neq),
        Box::new(comparison::Lt),
        Box::new(comparison::LtEq),
        Box::new(comparison::Gt),
        Box::new(comparison::GtEq),
        // Numeric
        Box::new(numeric::Ceil),
        Box::new(numeric::Floor),
        Box::new(numeric::IsNan),
        // String
        Box::new(string::Repeat),
        // Struct
        Box::new(struct_funcs::StructPack),
        // Unary
        Box::new(negate::Negate),
    ]
});

/// A function pointer with the concrete implementation of a scalar function.
pub type ScalarFn = fn(&[&Arc<Array>]) -> Result<Array>;

/// A generic scalar function that can specialize into a more specific function
/// depending on input types.
///
/// Generic scalar functions must be cheaply cloneable.
pub trait GenericScalarFunction: FunctionInfo + Debug + Sync + Send + DynClone {
    /// Specialize the function using the given input types.
    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>>;
}

impl Clone for Box<dyn GenericScalarFunction> {
    fn clone(&self) -> Self {
        dyn_clone::clone_box(&**self)
    }
}

impl PartialEq<dyn GenericScalarFunction> for Box<dyn GenericScalarFunction + '_> {
    fn eq(&self, other: &dyn GenericScalarFunction) -> bool {
        self.as_ref() == other
    }
}

impl PartialEq for dyn GenericScalarFunction + '_ {
    fn eq(&self, other: &dyn GenericScalarFunction) -> bool {
        self.name() == other.name() && self.signatures() == other.signatures()
    }
}

/// A specialized scalar function.
///
/// We're using a trait instead of returning the function pointer directly from
/// `GenericScalarFunction` because this will be what's serialized when
/// serializing pipelines for distributed execution.
pub trait SpecializedScalarFunction: Debug + Sync + Send + DynClone {
    /// Return the function pointer that implements this scalar function.
    fn function_impl(&self) -> ScalarFn;
}

impl Clone for Box<dyn SpecializedScalarFunction> {
    fn clone(&self) -> Self {
        dyn_clone::clone_box(&**self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanity_eq_check() {
        let fn1 = Box::new(arith::Add) as Box<dyn GenericScalarFunction>;
        let fn2 = Box::new(arith::Sub) as Box<dyn GenericScalarFunction>;
        let fn3 = Box::new(arith::Sub) as Box<dyn GenericScalarFunction>;

        assert_ne!(fn1, fn2);
        assert_eq!(fn2, fn3);
    }
}
