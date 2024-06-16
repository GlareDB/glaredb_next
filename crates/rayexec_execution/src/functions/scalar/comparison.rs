use super::{GenericScalarFunction, ScalarFn, SpecializedScalarFunction};
use crate::functions::{
    invalid_input_types_error, specialize_check_num_args, FunctionInfo, Signature,
};
use rayexec_bullet::array::Array;
use rayexec_bullet::array::{BooleanArray, BooleanValuesBuffer};
use rayexec_bullet::datatype::DataType;
use rayexec_bullet::executor::scalar::BinaryExecutor;
use rayexec_error::Result;
use std::fmt::Debug;
use std::sync::Arc;

const COMPARISON_SIGNATURES: &[Signature] = &[
    Signature {
        input: &[DataType::Boolean, DataType::Boolean],
        return_type: DataType::Boolean,
    },
    Signature {
        input: &[DataType::Float32, DataType::Float32],
        return_type: DataType::Boolean,
    },
    Signature {
        input: &[DataType::Float64, DataType::Float64],
        return_type: DataType::Boolean,
    },
    Signature {
        input: &[DataType::Int8, DataType::Int8],
        return_type: DataType::Boolean,
    },
    Signature {
        input: &[DataType::Int16, DataType::Int16],
        return_type: DataType::Boolean,
    },
    Signature {
        input: &[DataType::Int32, DataType::Int32],
        return_type: DataType::Boolean,
    },
    Signature {
        input: &[DataType::Int64, DataType::Int64],
        return_type: DataType::Boolean,
    },
    Signature {
        input: &[DataType::UInt8, DataType::UInt8],
        return_type: DataType::Boolean,
    },
    Signature {
        input: &[DataType::UInt16, DataType::UInt16],
        return_type: DataType::Boolean,
    },
    Signature {
        input: &[DataType::UInt32, DataType::UInt32],
        return_type: DataType::Boolean,
    },
    Signature {
        input: &[DataType::UInt64, DataType::UInt64],
        return_type: DataType::Boolean,
    },
    Signature {
        input: &[DataType::Date32, DataType::Date32],
        return_type: DataType::Boolean,
    },
    Signature {
        input: &[DataType::Utf8, DataType::Utf8],
        return_type: DataType::Boolean,
    },
    Signature {
        input: &[DataType::LargeUtf8, DataType::LargeUtf8],
        return_type: DataType::Boolean,
    },
    Signature {
        input: &[DataType::Binary, DataType::Binary],
        return_type: DataType::Boolean,
    },
    Signature {
        input: &[DataType::LargeBinary, DataType::LargeBinary],
        return_type: DataType::Boolean,
    },
];

macro_rules! generate_specialized_comparison {
    ($name:ident, $first_variant:ident, $second_variant:ident, $operation:expr) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub struct $name;

        impl SpecializedScalarFunction for $name {
            fn function_impl(&self) -> ScalarFn {
                fn inner(arrays: &[&Arc<Array>]) -> Result<Array> {
                    let first = arrays[0];
                    let second = arrays[1];
                    Ok(match (first.as_ref(), second.as_ref()) {
                        (Array::$first_variant(first), Array::$second_variant(second)) => {
                            let mut buffer = BooleanValuesBuffer::with_capacity(first.len());
                            let validity =
                                BinaryExecutor::execute(first, second, $operation, &mut buffer)?;
                            Array::Boolean(BooleanArray::new(buffer, validity))
                        }
                        other => panic!("unexpected array type: {other:?}"),
                    })
                }

                inner
            }
        }
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Eq;

impl FunctionInfo for Eq {
    fn name(&self) -> &'static str {
        "="
    }

    fn signatures(&self) -> &[Signature] {
        COMPARISON_SIGNATURES
    }
}

impl GenericScalarFunction for Eq {
    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        specialize_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Float32, DataType::Float32) => Ok(Box::new(EqFloat32)),
            (DataType::Float64, DataType::Float64) => Ok(Box::new(EqFloat64)),
            (DataType::Int8, DataType::Int8) => Ok(Box::new(EqInt8)),
            (DataType::Int16, DataType::Int16) => Ok(Box::new(EqInt16)),
            (DataType::Int32, DataType::Int32) => Ok(Box::new(EqInt32)),
            (DataType::Int64, DataType::Int64) => Ok(Box::new(EqInt64)),
            (DataType::UInt8, DataType::UInt8) => Ok(Box::new(EqUInt8)),
            (DataType::UInt16, DataType::UInt16) => Ok(Box::new(EqUInt16)),
            (DataType::UInt32, DataType::UInt32) => Ok(Box::new(EqUInt32)),
            (DataType::UInt64, DataType::UInt64) => Ok(Box::new(EqUInt64)),
            (DataType::Date32, DataType::Date32) => Ok(Box::new(EqDate32)),
            (DataType::Utf8, DataType::Utf8) => Ok(Box::new(EqUtf8)),
            (DataType::LargeUtf8, DataType::LargeUtf8) => Ok(Box::new(EqLargeUtf8)),
            (DataType::Binary, DataType::Binary) => Ok(Box::new(EqBinary)),
            (DataType::LargeBinary, DataType::LargeBinary) => Ok(Box::new(EqLargeBinary)),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

generate_specialized_comparison!(EqFloat32, Float32, Float32, |a, b| a == b);
generate_specialized_comparison!(EqFloat64, Float64, Float64, |a, b| a == b);
generate_specialized_comparison!(EqInt8, Int8, Int8, |a, b| a == b);
generate_specialized_comparison!(EqInt16, Int16, Int16, |a, b| a == b);
generate_specialized_comparison!(EqInt32, Int32, Int32, |a, b| a == b);
generate_specialized_comparison!(EqInt64, Int64, Int64, |a, b| a == b);
generate_specialized_comparison!(EqUInt8, UInt8, UInt8, |a, b| a == b);
generate_specialized_comparison!(EqUInt16, UInt16, UInt16, |a, b| a == b);
generate_specialized_comparison!(EqUInt32, UInt32, UInt32, |a, b| a == b);
generate_specialized_comparison!(EqUInt64, UInt64, UInt64, |a, b| a == b);
generate_specialized_comparison!(EqDate32, Date32, Date32, |a, b| a == b);
generate_specialized_comparison!(EqUtf8, Utf8, Utf8, |a, b| a == b);
generate_specialized_comparison!(EqLargeUtf8, LargeUtf8, LargeUtf8, |a, b| a == b);
generate_specialized_comparison!(EqBinary, Binary, Binary, |a, b| a == b);
generate_specialized_comparison!(EqLargeBinary, LargeBinary, LargeBinary, |a, b| a == b);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Neq;

impl FunctionInfo for Neq {
    fn name(&self) -> &'static str {
        "<>"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["!="]
    }

    fn signatures(&self) -> &[Signature] {
        COMPARISON_SIGNATURES
    }
}

impl GenericScalarFunction for Neq {
    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        specialize_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Float32, DataType::Float32) => Ok(Box::new(NeqFloat32)),
            (DataType::Float64, DataType::Float64) => Ok(Box::new(NeqFloat64)),
            (DataType::Int8, DataType::Int8) => Ok(Box::new(NeqInt8)),
            (DataType::Int16, DataType::Int16) => Ok(Box::new(NeqInt16)),
            (DataType::Int32, DataType::Int32) => Ok(Box::new(NeqInt32)),
            (DataType::Int64, DataType::Int64) => Ok(Box::new(NeqInt64)),
            (DataType::UInt8, DataType::UInt8) => Ok(Box::new(NeqUInt8)),
            (DataType::UInt16, DataType::UInt16) => Ok(Box::new(NeqUInt16)),
            (DataType::UInt32, DataType::UInt32) => Ok(Box::new(NeqUInt32)),
            (DataType::UInt64, DataType::UInt64) => Ok(Box::new(NeqUInt64)),
            (DataType::Date32, DataType::Date32) => Ok(Box::new(NeqDate32)),
            (DataType::Utf8, DataType::Utf8) => Ok(Box::new(NeqUtf8)),
            (DataType::LargeUtf8, DataType::LargeUtf8) => Ok(Box::new(NeqLargeUtf8)),
            (DataType::Binary, DataType::Binary) => Ok(Box::new(NeqBinary)),
            (DataType::LargeBinary, DataType::LargeBinary) => Ok(Box::new(NeqLargeBinary)),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

generate_specialized_comparison!(NeqFloat32, Float32, Float32, |a, b| a != b);
generate_specialized_comparison!(NeqFloat64, Float64, Float64, |a, b| a != b);
generate_specialized_comparison!(NeqInt8, Int8, Int8, |a, b| a != b);
generate_specialized_comparison!(NeqInt16, Int16, Int16, |a, b| a != b);
generate_specialized_comparison!(NeqInt32, Int32, Int32, |a, b| a != b);
generate_specialized_comparison!(NeqInt64, Int64, Int64, |a, b| a != b);
generate_specialized_comparison!(NeqUInt8, UInt8, UInt8, |a, b| a != b);
generate_specialized_comparison!(NeqUInt16, UInt16, UInt16, |a, b| a != b);
generate_specialized_comparison!(NeqUInt32, UInt32, UInt32, |a, b| a != b);
generate_specialized_comparison!(NeqUInt64, UInt64, UInt64, |a, b| a != b);
generate_specialized_comparison!(NeqDate32, Date32, Date32, |a, b| a != b);
generate_specialized_comparison!(NeqUtf8, Utf8, Utf8, |a, b| a != b);
generate_specialized_comparison!(NeqLargeUtf8, LargeUtf8, LargeUtf8, |a, b| a != b);
generate_specialized_comparison!(NeqBinary, Binary, Binary, |a, b| a != b);
generate_specialized_comparison!(NeqLargeBinary, LargeBinary, LargeBinary, |a, b| a != b);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lt;

impl FunctionInfo for Lt {
    fn name(&self) -> &'static str {
        "<"
    }

    fn signatures(&self) -> &[Signature] {
        COMPARISON_SIGNATURES
    }
}

impl GenericScalarFunction for Lt {
    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        specialize_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Float32, DataType::Float32) => Ok(Box::new(LtFloat32)),
            (DataType::Float64, DataType::Float64) => Ok(Box::new(LtFloat64)),
            (DataType::Int8, DataType::Int8) => Ok(Box::new(LtInt8)),
            (DataType::Int16, DataType::Int16) => Ok(Box::new(LtInt16)),
            (DataType::Int32, DataType::Int32) => Ok(Box::new(LtInt32)),
            (DataType::Int64, DataType::Int64) => Ok(Box::new(LtInt64)),
            (DataType::UInt8, DataType::UInt8) => Ok(Box::new(LtUInt8)),
            (DataType::UInt16, DataType::UInt16) => Ok(Box::new(LtUInt16)),
            (DataType::UInt32, DataType::UInt32) => Ok(Box::new(LtUInt32)),
            (DataType::UInt64, DataType::UInt64) => Ok(Box::new(LtUInt64)),
            (DataType::Date32, DataType::Date32) => Ok(Box::new(LtDate32)),
            (DataType::Utf8, DataType::Utf8) => Ok(Box::new(LtUtf8)),
            (DataType::LargeUtf8, DataType::LargeUtf8) => Ok(Box::new(LtLargeUtf8)),
            (DataType::Binary, DataType::Binary) => Ok(Box::new(LtBinary)),
            (DataType::LargeBinary, DataType::LargeBinary) => Ok(Box::new(LtLargeBinary)),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

generate_specialized_comparison!(LtFloat32, Float32, Float32, |a, b| a < b);
generate_specialized_comparison!(LtFloat64, Float64, Float64, |a, b| a < b);
generate_specialized_comparison!(LtInt8, Int8, Int8, |a, b| a < b);
generate_specialized_comparison!(LtInt16, Int16, Int16, |a, b| a < b);
generate_specialized_comparison!(LtInt32, Int32, Int32, |a, b| a < b);
generate_specialized_comparison!(LtInt64, Int64, Int64, |a, b| a < b);
generate_specialized_comparison!(LtUInt8, UInt8, UInt8, |a, b| a < b);
generate_specialized_comparison!(LtUInt16, UInt16, UInt16, |a, b| a < b);
generate_specialized_comparison!(LtUInt32, UInt32, UInt32, |a, b| a < b);
generate_specialized_comparison!(LtUInt64, UInt64, UInt64, |a, b| a < b);
generate_specialized_comparison!(LtDate32, Date32, Date32, |a, b| a < b);
generate_specialized_comparison!(LtUtf8, Utf8, Utf8, |a, b| a < b);
generate_specialized_comparison!(LtLargeUtf8, LargeUtf8, LargeUtf8, |a, b| a < b);
generate_specialized_comparison!(LtBinary, Binary, Binary, |a, b| a < b);
generate_specialized_comparison!(LtLargeBinary, LargeBinary, LargeBinary, |a, b| a < b);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LtEq;

impl FunctionInfo for LtEq {
    fn name(&self) -> &'static str {
        "<="
    }

    fn signatures(&self) -> &[Signature] {
        COMPARISON_SIGNATURES
    }
}

impl GenericScalarFunction for LtEq {
    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        specialize_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Float32, DataType::Float32) => Ok(Box::new(LtEqFloat32)),
            (DataType::Float64, DataType::Float64) => Ok(Box::new(LtEqFloat64)),
            (DataType::Int8, DataType::Int8) => Ok(Box::new(LtEqInt8)),
            (DataType::Int16, DataType::Int16) => Ok(Box::new(LtEqInt16)),
            (DataType::Int32, DataType::Int32) => Ok(Box::new(LtEqInt32)),
            (DataType::Int64, DataType::Int64) => Ok(Box::new(LtEqInt64)),
            (DataType::UInt8, DataType::UInt8) => Ok(Box::new(LtEqUInt8)),
            (DataType::UInt16, DataType::UInt16) => Ok(Box::new(LtEqUInt16)),
            (DataType::UInt32, DataType::UInt32) => Ok(Box::new(LtEqUInt32)),
            (DataType::UInt64, DataType::UInt64) => Ok(Box::new(LtEqUInt64)),
            (DataType::Date32, DataType::Date32) => Ok(Box::new(LtEqDate32)),
            (DataType::Utf8, DataType::Utf8) => Ok(Box::new(LtEqUtf8)),
            (DataType::LargeUtf8, DataType::LargeUtf8) => Ok(Box::new(LtEqLargeUtf8)),
            (DataType::Binary, DataType::Binary) => Ok(Box::new(LtEqBinary)),
            (DataType::LargeBinary, DataType::LargeBinary) => Ok(Box::new(LtEqLargeBinary)),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

generate_specialized_comparison!(LtEqFloat32, Float32, Float32, |a, b| a <= b);
generate_specialized_comparison!(LtEqFloat64, Float64, Float64, |a, b| a <= b);
generate_specialized_comparison!(LtEqInt8, Int8, Int8, |a, b| a <= b);
generate_specialized_comparison!(LtEqInt16, Int16, Int16, |a, b| a <= b);
generate_specialized_comparison!(LtEqInt32, Int32, Int32, |a, b| a <= b);
generate_specialized_comparison!(LtEqInt64, Int64, Int64, |a, b| a <= b);
generate_specialized_comparison!(LtEqUInt8, UInt8, UInt8, |a, b| a <= b);
generate_specialized_comparison!(LtEqUInt16, UInt16, UInt16, |a, b| a <= b);
generate_specialized_comparison!(LtEqUInt32, UInt32, UInt32, |a, b| a <= b);
generate_specialized_comparison!(LtEqUInt64, UInt64, UInt64, |a, b| a <= b);
generate_specialized_comparison!(LtEqDate32, Date32, Date32, |a, b| a <= b);
generate_specialized_comparison!(LtEqUtf8, Utf8, Utf8, |a, b| a <= b);
generate_specialized_comparison!(LtEqLargeUtf8, LargeUtf8, LargeUtf8, |a, b| a <= b);
generate_specialized_comparison!(LtEqBinary, Binary, Binary, |a, b| a <= b);
generate_specialized_comparison!(LtEqLargeBinary, LargeBinary, LargeBinary, |a, b| a <= b);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Gt;

impl FunctionInfo for Gt {
    fn name(&self) -> &'static str {
        ">"
    }

    fn signatures(&self) -> &[Signature] {
        COMPARISON_SIGNATURES
    }
}

impl GenericScalarFunction for Gt {
    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        specialize_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Float32, DataType::Float32) => Ok(Box::new(GtFloat32)),
            (DataType::Float64, DataType::Float64) => Ok(Box::new(GtFloat64)),
            (DataType::Int8, DataType::Int8) => Ok(Box::new(GtInt8)),
            (DataType::Int16, DataType::Int16) => Ok(Box::new(GtInt16)),
            (DataType::Int32, DataType::Int32) => Ok(Box::new(GtInt32)),
            (DataType::Int64, DataType::Int64) => Ok(Box::new(GtInt64)),
            (DataType::UInt8, DataType::UInt8) => Ok(Box::new(GtUInt8)),
            (DataType::UInt16, DataType::UInt16) => Ok(Box::new(GtUInt16)),
            (DataType::UInt32, DataType::UInt32) => Ok(Box::new(GtUInt32)),
            (DataType::UInt64, DataType::UInt64) => Ok(Box::new(GtUInt64)),
            (DataType::Date32, DataType::Date32) => Ok(Box::new(GtDate32)),
            (DataType::Utf8, DataType::Utf8) => Ok(Box::new(GtUtf8)),
            (DataType::LargeUtf8, DataType::LargeUtf8) => Ok(Box::new(GtLargeUtf8)),
            (DataType::Binary, DataType::Binary) => Ok(Box::new(GtBinary)),
            (DataType::LargeBinary, DataType::LargeBinary) => Ok(Box::new(GtLargeBinary)),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

generate_specialized_comparison!(GtFloat32, Float32, Float32, |a, b| a > b);
generate_specialized_comparison!(GtFloat64, Float64, Float64, |a, b| a > b);
generate_specialized_comparison!(GtInt8, Int8, Int8, |a, b| a > b);
generate_specialized_comparison!(GtInt16, Int16, Int16, |a, b| a > b);
generate_specialized_comparison!(GtInt32, Int32, Int32, |a, b| a > b);
generate_specialized_comparison!(GtInt64, Int64, Int64, |a, b| a > b);
generate_specialized_comparison!(GtUInt8, UInt8, UInt8, |a, b| a > b);
generate_specialized_comparison!(GtUInt16, UInt16, UInt16, |a, b| a > b);
generate_specialized_comparison!(GtUInt32, UInt32, UInt32, |a, b| a > b);
generate_specialized_comparison!(GtUInt64, UInt64, UInt64, |a, b| a > b);
generate_specialized_comparison!(GtDate32, Date32, Date32, |a, b| a > b);
generate_specialized_comparison!(GtUtf8, Utf8, Utf8, |a, b| a > b);
generate_specialized_comparison!(GtLargeUtf8, LargeUtf8, LargeUtf8, |a, b| a > b);
generate_specialized_comparison!(GtBinary, Binary, Binary, |a, b| a > b);
generate_specialized_comparison!(GtLargeBinary, LargeBinary, LargeBinary, |a, b| a > b);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GtEq;

impl FunctionInfo for GtEq {
    fn name(&self) -> &'static str {
        ">="
    }

    fn signatures(&self) -> &[Signature] {
        COMPARISON_SIGNATURES
    }
}

impl GenericScalarFunction for GtEq {
    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        specialize_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Float32, DataType::Float32) => Ok(Box::new(GtEqFloat32)),
            (DataType::Float64, DataType::Float64) => Ok(Box::new(GtEqFloat64)),
            (DataType::Int8, DataType::Int8) => Ok(Box::new(GtEqInt8)),
            (DataType::Int16, DataType::Int16) => Ok(Box::new(GtEqInt16)),
            (DataType::Int32, DataType::Int32) => Ok(Box::new(GtEqInt32)),
            (DataType::Int64, DataType::Int64) => Ok(Box::new(GtEqInt64)),
            (DataType::UInt8, DataType::UInt8) => Ok(Box::new(GtEqUInt8)),
            (DataType::UInt16, DataType::UInt16) => Ok(Box::new(GtEqUInt16)),
            (DataType::UInt32, DataType::UInt32) => Ok(Box::new(GtEqUInt32)),
            (DataType::UInt64, DataType::UInt64) => Ok(Box::new(GtEqUInt64)),
            (DataType::Date32, DataType::Date32) => Ok(Box::new(GtEqDate32)),
            (DataType::Utf8, DataType::Utf8) => Ok(Box::new(GtEqUtf8)),
            (DataType::LargeUtf8, DataType::LargeUtf8) => Ok(Box::new(GtEqLargeUtf8)),
            (DataType::Binary, DataType::Binary) => Ok(Box::new(GtEqBinary)),
            (DataType::LargeBinary, DataType::LargeBinary) => Ok(Box::new(GtEqLargeBinary)),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

generate_specialized_comparison!(GtEqFloat32, Float32, Float32, |a, b| a >= b);
generate_specialized_comparison!(GtEqFloat64, Float64, Float64, |a, b| a >= b);
generate_specialized_comparison!(GtEqInt8, Int8, Int8, |a, b| a >= b);
generate_specialized_comparison!(GtEqInt16, Int16, Int16, |a, b| a >= b);
generate_specialized_comparison!(GtEqInt32, Int32, Int32, |a, b| a >= b);
generate_specialized_comparison!(GtEqInt64, Int64, Int64, |a, b| a >= b);
generate_specialized_comparison!(GtEqUInt8, UInt8, UInt8, |a, b| a >= b);
generate_specialized_comparison!(GtEqUInt16, UInt16, UInt16, |a, b| a >= b);
generate_specialized_comparison!(GtEqUInt32, UInt32, UInt32, |a, b| a >= b);
generate_specialized_comparison!(GtEqUInt64, UInt64, UInt64, |a, b| a >= b);
generate_specialized_comparison!(GtEqDate32, Date32, Date32, |a, b| a >= b);
generate_specialized_comparison!(GtEqUtf8, Utf8, Utf8, |a, b| a >= b);
generate_specialized_comparison!(GtEqLargeUtf8, LargeUtf8, LargeUtf8, |a, b| a >= b);
generate_specialized_comparison!(GtEqBinary, Binary, Binary, |a, b| a >= b);
generate_specialized_comparison!(GtEqLargeBinary, LargeBinary, LargeBinary, |a, b| a >= b);

#[cfg(test)]
mod tests {
    use rayexec_bullet::array::{BooleanArray, Int32Array};

    use super::*;

    #[test]
    fn eq_i32() {
        let a = Arc::new(Array::Int32(Int32Array::from_iter([1, 2, 3])));
        let b = Arc::new(Array::Int32(Int32Array::from_iter([2, 2, 6])));

        let specialized = Eq.specialize(&[DataType::Int32, DataType::Int32]).unwrap();

        let out = (specialized.function_impl())(&[&a, &b]).unwrap();
        let expected = Array::Boolean(BooleanArray::from_iter([false, true, false]));

        assert_eq!(expected, out);
    }

    #[test]
    fn neq_i32() {
        let a = Arc::new(Array::Int32(Int32Array::from_iter([1, 2, 3])));
        let b = Arc::new(Array::Int32(Int32Array::from_iter([2, 2, 6])));

        let specialized = Neq.specialize(&[DataType::Int32, DataType::Int32]).unwrap();

        let out = (specialized.function_impl())(&[&a, &b]).unwrap();
        let expected = Array::Boolean(BooleanArray::from_iter([true, false, true]));

        assert_eq!(expected, out);
    }

    #[test]
    fn lt_i32() {
        let a = Arc::new(Array::Int32(Int32Array::from_iter([1, 2, 3])));
        let b = Arc::new(Array::Int32(Int32Array::from_iter([2, 2, 6])));

        let specialized = Lt.specialize(&[DataType::Int32, DataType::Int32]).unwrap();

        let out = (specialized.function_impl())(&[&a, &b]).unwrap();
        let expected = Array::Boolean(BooleanArray::from_iter([true, false, true]));

        assert_eq!(expected, out);
    }

    #[test]
    fn lt_eq_i32() {
        let a = Arc::new(Array::Int32(Int32Array::from_iter([1, 2, 3])));
        let b = Arc::new(Array::Int32(Int32Array::from_iter([2, 2, 6])));

        let specialized = LtEq
            .specialize(&[DataType::Int32, DataType::Int32])
            .unwrap();

        let out = (specialized.function_impl())(&[&a, &b]).unwrap();
        let expected = Array::Boolean(BooleanArray::from_iter([true, true, true]));

        assert_eq!(expected, out);
    }

    #[test]
    fn gt_i32() {
        let a = Arc::new(Array::Int32(Int32Array::from_iter([1, 2, 3])));
        let b = Arc::new(Array::Int32(Int32Array::from_iter([2, 2, 6])));

        let specialized = Gt.specialize(&[DataType::Int32, DataType::Int32]).unwrap();

        let out = (specialized.function_impl())(&[&a, &b]).unwrap();
        let expected = Array::Boolean(BooleanArray::from_iter([false, false, false]));

        assert_eq!(expected, out);
    }

    #[test]
    fn gt_eq_i32() {
        let a = Arc::new(Array::Int32(Int32Array::from_iter([1, 2, 3])));
        let b = Arc::new(Array::Int32(Int32Array::from_iter([2, 2, 6])));

        let specialized = GtEq
            .specialize(&[DataType::Int32, DataType::Int32])
            .unwrap();

        let out = (specialized.function_impl())(&[&a, &b]).unwrap();
        let expected = Array::Boolean(BooleanArray::from_iter([false, true, false]));

        assert_eq!(expected, out);
    }
}
