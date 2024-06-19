use crate::functions::scalar::macros::{
    primitive_binary_execute, primitive_binary_execute_no_wrap,
};
use crate::logical::operator::LogicalExpression;

use super::{
    GenericScalarFunction, PlannedScalarFunction, ScalarFunction, ScalarFunctionSet,
    SpecializedScalarFunction,
};
use rayexec_bullet::array::{Array, Decimal128Array, Decimal64Array};
use rayexec_bullet::datatype::{
    DataType, DataTypeId, PrimitiveType, PrimitiveTypeDate32, PrimitiveTypeDate64,
    PrimitiveTypeFloat32, PrimitiveTypeFloat64, PrimitiveTypeInt16, PrimitiveTypeInt32,
    PrimitiveTypeInt64, PrimitiveTypeInt8, PrimitiveTypeTimestampMicroseconds,
    PrimitiveTypeTimestampMilliseconds, PrimitiveTypeTimestampNanoseconds,
    PrimitiveTypeTimestampSeconds, PrimitiveTypeUInt16, PrimitiveTypeUInt32, PrimitiveTypeUInt64,
    PrimitiveTypeUInt8,
};
use rayexec_bullet::field::TypeSchema;
use rayexec_error::Result;
use std::fmt::Debug;
use std::sync::Arc;

#[rustfmt::skip]
pub fn add_scalar_function_set() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::with_aliases("+", &["add"]);

    set.add_function(&[DataTypeId::Int8, DataTypeId::Int8], Box::new(AddPrimitiveFunction::new(PrimitiveTypeInt8)));
    set.add_function(&[DataTypeId::Int16, DataTypeId::Int16], Box::new(AddPrimitiveFunction::new(PrimitiveTypeInt16)));
    set.add_function(&[DataTypeId::Int32, DataTypeId::Int32], Box::new(AddPrimitiveFunction::new(PrimitiveTypeInt32)));
    set.add_function(&[DataTypeId::Int64, DataTypeId::Int64], Box::new(AddPrimitiveFunction::new(PrimitiveTypeInt64)));
    set.add_function(&[DataTypeId::UInt8, DataTypeId::UInt8], Box::new(AddPrimitiveFunction::new(PrimitiveTypeUInt8)));
    set.add_function(&[DataTypeId::UInt16, DataTypeId::UInt16], Box::new(AddPrimitiveFunction::new(PrimitiveTypeUInt16)));
    set.add_function(&[DataTypeId::UInt32, DataTypeId::UInt32], Box::new(AddPrimitiveFunction::new(PrimitiveTypeUInt32)));
    set.add_function(&[DataTypeId::UInt64, DataTypeId::UInt64], Box::new(AddPrimitiveFunction::new(PrimitiveTypeUInt64)));
    set.add_function(&[DataTypeId::Float32, DataTypeId::Float32], Box::new(AddPrimitiveFunction::new(PrimitiveTypeFloat32)));
    set.add_function(&[DataTypeId::Float64, DataTypeId::Float64], Box::new(AddPrimitiveFunction::new(PrimitiveTypeFloat64)));
    set.add_function(&[DataTypeId::TimestampSeconds, DataTypeId::TimestampSeconds], Box::new(AddPrimitiveFunction::new(PrimitiveTypeTimestampSeconds)));
    set.add_function(&[DataTypeId::TimestampMilliseconds, DataTypeId::TimestampMilliseconds], Box::new(AddPrimitiveFunction::new(PrimitiveTypeTimestampMilliseconds)));
    set.add_function(&[DataTypeId::TimestampMicroseconds, DataTypeId::TimestampMicroseconds], Box::new(AddPrimitiveFunction::new(PrimitiveTypeTimestampMicroseconds)));
    set.add_function(&[DataTypeId::TimestampNanoseconds, DataTypeId::TimestampNanoseconds], Box::new(AddPrimitiveFunction::new(PrimitiveTypeTimestampNanoseconds)));
    set.add_function(&[DataTypeId::Date32, DataTypeId::Date32], Box::new(AddPrimitiveFunction::new(PrimitiveTypeDate32)));
    set.add_function(&[DataTypeId::Date64, DataTypeId::Date64], Box::new(AddPrimitiveFunction::new(PrimitiveTypeDate64)));

    set
}

#[rustfmt::skip]
pub fn sub_scalar_function_set() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::with_aliases("-", &["sub"]);

    set.add_function(&[DataTypeId::Int8, DataTypeId::Int8], Box::new(SubPrimitiveFunction::new(PrimitiveTypeInt8)));
    set.add_function(&[DataTypeId::Int16, DataTypeId::Int16], Box::new(SubPrimitiveFunction::new(PrimitiveTypeInt16)));
    set.add_function(&[DataTypeId::Int32, DataTypeId::Int32], Box::new(SubPrimitiveFunction::new(PrimitiveTypeInt32)));
    set.add_function(&[DataTypeId::Int64, DataTypeId::Int64], Box::new(SubPrimitiveFunction::new(PrimitiveTypeInt64)));
    set.add_function(&[DataTypeId::UInt8, DataTypeId::UInt8], Box::new(SubPrimitiveFunction::new(PrimitiveTypeUInt8)));
    set.add_function(&[DataTypeId::UInt16, DataTypeId::UInt16], Box::new(SubPrimitiveFunction::new(PrimitiveTypeUInt16)));
    set.add_function(&[DataTypeId::UInt32, DataTypeId::UInt32], Box::new(SubPrimitiveFunction::new(PrimitiveTypeUInt32)));
    set.add_function(&[DataTypeId::UInt64, DataTypeId::UInt64], Box::new(SubPrimitiveFunction::new(PrimitiveTypeUInt64)));
    set.add_function(&[DataTypeId::Float32, DataTypeId::Float32], Box::new(SubPrimitiveFunction::new(PrimitiveTypeFloat32)));
    set.add_function(&[DataTypeId::Float64, DataTypeId::Float64], Box::new(SubPrimitiveFunction::new(PrimitiveTypeFloat64)));
    set.add_function(&[DataTypeId::TimestampSeconds, DataTypeId::TimestampSeconds], Box::new(SubPrimitiveFunction::new(PrimitiveTypeTimestampSeconds)));
    set.add_function(&[DataTypeId::TimestampMilliseconds, DataTypeId::TimestampMilliseconds], Box::new(SubPrimitiveFunction::new(PrimitiveTypeTimestampMilliseconds)));
    set.add_function(&[DataTypeId::TimestampMicroseconds, DataTypeId::TimestampMicroseconds], Box::new(SubPrimitiveFunction::new(PrimitiveTypeTimestampMicroseconds)));
    set.add_function(&[DataTypeId::TimestampNanoseconds, DataTypeId::TimestampNanoseconds], Box::new(SubPrimitiveFunction::new(PrimitiveTypeTimestampNanoseconds)));
    set.add_function(&[DataTypeId::Date32, DataTypeId::Date32], Box::new(SubPrimitiveFunction::new(PrimitiveTypeDate32)));
    set.add_function(&[DataTypeId::Date64, DataTypeId::Date64], Box::new(SubPrimitiveFunction::new(PrimitiveTypeDate64)));

    set
}

#[rustfmt::skip]
pub fn mul_scalar_function_set() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::with_aliases("*", &["mul"]);

    set.add_function(&[DataTypeId::Int8, DataTypeId::Int8], Box::new(MulPrimitiveFunction::new(PrimitiveTypeInt8)));
    set.add_function(&[DataTypeId::Int16, DataTypeId::Int16], Box::new(MulPrimitiveFunction::new(PrimitiveTypeInt16)));
    set.add_function(&[DataTypeId::Int32, DataTypeId::Int32], Box::new(MulPrimitiveFunction::new(PrimitiveTypeInt32)));
    set.add_function(&[DataTypeId::Int64, DataTypeId::Int64], Box::new(MulPrimitiveFunction::new(PrimitiveTypeInt64)));
    set.add_function(&[DataTypeId::UInt8, DataTypeId::UInt8], Box::new(MulPrimitiveFunction::new(PrimitiveTypeUInt8)));
    set.add_function(&[DataTypeId::UInt16, DataTypeId::UInt16], Box::new(MulPrimitiveFunction::new(PrimitiveTypeUInt16)));
    set.add_function(&[DataTypeId::UInt32, DataTypeId::UInt32], Box::new(MulPrimitiveFunction::new(PrimitiveTypeUInt32)));
    set.add_function(&[DataTypeId::UInt64, DataTypeId::UInt64], Box::new(MulPrimitiveFunction::new(PrimitiveTypeUInt64)));
    set.add_function(&[DataTypeId::Float32, DataTypeId::Float32], Box::new(MulPrimitiveFunction::new(PrimitiveTypeFloat32)));
    set.add_function(&[DataTypeId::Float64, DataTypeId::Float64], Box::new(MulPrimitiveFunction::new(PrimitiveTypeFloat64)));
    set.add_function(&[DataTypeId::TimestampSeconds, DataTypeId::TimestampSeconds], Box::new(MulPrimitiveFunction::new(PrimitiveTypeTimestampSeconds)));
    set.add_function(&[DataTypeId::TimestampMilliseconds, DataTypeId::TimestampMilliseconds], Box::new(MulPrimitiveFunction::new(PrimitiveTypeTimestampMilliseconds)));
    set.add_function(&[DataTypeId::TimestampMicroseconds, DataTypeId::TimestampMicroseconds], Box::new(MulPrimitiveFunction::new(PrimitiveTypeTimestampMicroseconds)));
    set.add_function(&[DataTypeId::TimestampNanoseconds, DataTypeId::TimestampNanoseconds], Box::new(MulPrimitiveFunction::new(PrimitiveTypeTimestampNanoseconds)));
    set.add_function(&[DataTypeId::Date32, DataTypeId::Date32], Box::new(MulPrimitiveFunction::new(PrimitiveTypeDate32)));
    set.add_function(&[DataTypeId::Date64, DataTypeId::Date64], Box::new(MulPrimitiveFunction::new(PrimitiveTypeDate64)));

    set
}

#[rustfmt::skip]
pub fn div_scalar_function_set() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::with_aliases("/", &["div"]);

    set.add_function(&[DataTypeId::Int8, DataTypeId::Int8], Box::new(DivPrimitiveFunction::new(PrimitiveTypeInt8)));
    set.add_function(&[DataTypeId::Int16, DataTypeId::Int16], Box::new(DivPrimitiveFunction::new(PrimitiveTypeInt16)));
    set.add_function(&[DataTypeId::Int32, DataTypeId::Int32], Box::new(DivPrimitiveFunction::new(PrimitiveTypeInt32)));
    set.add_function(&[DataTypeId::Int64, DataTypeId::Int64], Box::new(DivPrimitiveFunction::new(PrimitiveTypeInt64)));
    set.add_function(&[DataTypeId::UInt8, DataTypeId::UInt8], Box::new(DivPrimitiveFunction::new(PrimitiveTypeUInt8)));
    set.add_function(&[DataTypeId::UInt16, DataTypeId::UInt16], Box::new(DivPrimitiveFunction::new(PrimitiveTypeUInt16)));
    set.add_function(&[DataTypeId::UInt32, DataTypeId::UInt32], Box::new(DivPrimitiveFunction::new(PrimitiveTypeUInt32)));
    set.add_function(&[DataTypeId::UInt64, DataTypeId::UInt64], Box::new(DivPrimitiveFunction::new(PrimitiveTypeUInt64)));
    set.add_function(&[DataTypeId::Float32, DataTypeId::Float32], Box::new(DivPrimitiveFunction::new(PrimitiveTypeFloat32)));
    set.add_function(&[DataTypeId::Float64, DataTypeId::Float64], Box::new(DivPrimitiveFunction::new(PrimitiveTypeFloat64)));
    set.add_function(&[DataTypeId::TimestampSeconds, DataTypeId::TimestampSeconds], Box::new(DivPrimitiveFunction::new(PrimitiveTypeTimestampSeconds)));
    set.add_function(&[DataTypeId::TimestampMilliseconds, DataTypeId::TimestampMilliseconds], Box::new(DivPrimitiveFunction::new(PrimitiveTypeTimestampMilliseconds)));
    set.add_function(&[DataTypeId::TimestampMicroseconds, DataTypeId::TimestampMicroseconds], Box::new(DivPrimitiveFunction::new(PrimitiveTypeTimestampMicroseconds)));
    set.add_function(&[DataTypeId::TimestampNanoseconds, DataTypeId::TimestampNanoseconds], Box::new(DivPrimitiveFunction::new(PrimitiveTypeTimestampNanoseconds)));
    set.add_function(&[DataTypeId::Date32, DataTypeId::Date32], Box::new(DivPrimitiveFunction::new(PrimitiveTypeDate32)));
    set.add_function(&[DataTypeId::Date64, DataTypeId::Date64], Box::new(DivPrimitiveFunction::new(PrimitiveTypeDate64)));

    set
}

#[rustfmt::skip]
pub fn rem_scalar_function_set() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::with_aliases("%", &["rem"]);

    set.add_function(&[DataTypeId::Int8, DataTypeId::Int8], Box::new(RemPrimitiveFunction::new(PrimitiveTypeInt8)));
    set.add_function(&[DataTypeId::Int16, DataTypeId::Int16], Box::new(RemPrimitiveFunction::new(PrimitiveTypeInt16)));
    set.add_function(&[DataTypeId::Int32, DataTypeId::Int32], Box::new(RemPrimitiveFunction::new(PrimitiveTypeInt32)));
    set.add_function(&[DataTypeId::Int64, DataTypeId::Int64], Box::new(RemPrimitiveFunction::new(PrimitiveTypeInt64)));
    set.add_function(&[DataTypeId::UInt8, DataTypeId::UInt8], Box::new(RemPrimitiveFunction::new(PrimitiveTypeUInt8)));
    set.add_function(&[DataTypeId::UInt16, DataTypeId::UInt16], Box::new(RemPrimitiveFunction::new(PrimitiveTypeUInt16)));
    set.add_function(&[DataTypeId::UInt32, DataTypeId::UInt32], Box::new(RemPrimitiveFunction::new(PrimitiveTypeUInt32)));
    set.add_function(&[DataTypeId::UInt64, DataTypeId::UInt64], Box::new(RemPrimitiveFunction::new(PrimitiveTypeUInt64)));
    set.add_function(&[DataTypeId::Float32, DataTypeId::Float32], Box::new(RemPrimitiveFunction::new(PrimitiveTypeFloat32)));
    set.add_function(&[DataTypeId::Float64, DataTypeId::Float64], Box::new(RemPrimitiveFunction::new(PrimitiveTypeFloat64)));
    set.add_function(&[DataTypeId::TimestampSeconds, DataTypeId::TimestampSeconds], Box::new(RemPrimitiveFunction::new(PrimitiveTypeTimestampSeconds)));
    set.add_function(&[DataTypeId::TimestampMilliseconds, DataTypeId::TimestampMilliseconds], Box::new(RemPrimitiveFunction::new(PrimitiveTypeTimestampMilliseconds)));
    set.add_function(&[DataTypeId::TimestampMicroseconds, DataTypeId::TimestampMicroseconds], Box::new(RemPrimitiveFunction::new(PrimitiveTypeTimestampMicroseconds)));
    set.add_function(&[DataTypeId::TimestampNanoseconds, DataTypeId::TimestampNanoseconds], Box::new(RemPrimitiveFunction::new(PrimitiveTypeTimestampNanoseconds)));
    set.add_function(&[DataTypeId::Date32, DataTypeId::Date32], Box::new(RemPrimitiveFunction::new(PrimitiveTypeDate32)));
    set.add_function(&[DataTypeId::Date64, DataTypeId::Date64], Box::new(RemPrimitiveFunction::new(PrimitiveTypeDate64)));

    set
}

macro_rules! generate_primitive_arith_function {
    ($name:ident, $op:expr) => {
        fn $name(arrays: &[&Arc<Array>]) -> Result<Array> {
            let first = arrays[0];
            let second = arrays[1];
            Ok(match (first.as_ref(), second.as_ref()) {
                (Array::Int8(first), Array::Int8(second)) => {
                    primitive_binary_execute!(first, second, Int8, $op)
                }
                (Array::Int16(first), Array::Int16(second)) => {
                    primitive_binary_execute!(first, second, Int16, $op)
                }
                (Array::Int32(first), Array::Int32(second)) => {
                    primitive_binary_execute!(first, second, Int32, $op)
                }
                (Array::Int64(first), Array::Int64(second)) => {
                    primitive_binary_execute!(first, second, Int64, $op)
                }
                (Array::UInt8(first), Array::UInt8(second)) => {
                    primitive_binary_execute!(first, second, UInt8, $op)
                }
                (Array::UInt16(first), Array::UInt16(second)) => {
                    primitive_binary_execute!(first, second, UInt16, $op)
                }
                (Array::UInt32(first), Array::UInt32(second)) => {
                    primitive_binary_execute!(first, second, UInt32, $op)
                }
                (Array::UInt64(first), Array::UInt64(second)) => {
                    primitive_binary_execute!(first, second, UInt64, $op)
                }
                (Array::Float32(first), Array::Float32(second)) => {
                    primitive_binary_execute!(first, second, Float32, $op)
                }
                (Array::Float64(first), Array::Float64(second)) => {
                    primitive_binary_execute!(first, second, Float64, $op)
                }
                (Array::TimestampSeconds(first), Array::TimestampSeconds(second)) => {
                    primitive_binary_execute!(first, second, TimestampSeconds, $op)
                }
                (Array::TimestampMilliseconds(first), Array::TimestampMilliseconds(second)) => {
                    primitive_binary_execute!(first, second, TimestampMilliseconds, $op)
                }
                (Array::TimestampMicroseconds(first), Array::TimestampMicroseconds(second)) => {
                    primitive_binary_execute!(first, second, TimestampMicroseconds, $op)
                }
                (Array::TimestampNanoseconds(first), Array::TimestampNanoseconds(second)) => {
                    primitive_binary_execute!(first, second, TimestampNanoseconds, $op)
                }
                (Array::Date32(first), Array::Date32(second)) => {
                    primitive_binary_execute!(first, second, Date32, $op)
                }
                (Array::Date64(first), Array::Date64(second)) => {
                    primitive_binary_execute!(first, second, Date64, $op)
                }
                other => panic!("unexpected array type: {other:?}"),
            })
        }
    };
}

generate_primitive_arith_function!(prim_add, |a, b| a + b);
generate_primitive_arith_function!(prim_sub, |a, b| a - b);
generate_primitive_arith_function!(prim_div, |a, b| a / b);
generate_primitive_arith_function!(prim_mul, |a, b| a * b);
generate_primitive_arith_function!(prim_rem, |a, b| a % b);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AddPrimitiveFunction<P: PrimitiveType> {
    typ: P,
}

impl<P: PrimitiveType> AddPrimitiveFunction<P> {
    const fn new(typ: P) -> Self {
        AddPrimitiveFunction { typ }
    }
}

impl<P: PrimitiveType> ScalarFunction for AddPrimitiveFunction<P> {
    fn plan(
        &self,
        _inputs: &[LogicalExpression],
        _operator_schema: &TypeSchema,
    ) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(AddPrimitiveImpl { typ: self.typ }))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AddPrimitiveImpl<P: PrimitiveType> {
    typ: P,
}

impl<P: PrimitiveType> PlannedScalarFunction for AddPrimitiveImpl<P> {
    fn name(&self) -> &'static str {
        "add_primitive_impl"
    }

    fn return_type(&self) -> DataType {
        P::DATATYPE.clone()
    }

    fn execute(&self, inputs: &[&Arc<Array>]) -> Result<Array> {
        prim_add(inputs)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubPrimitiveFunction<P: PrimitiveType> {
    typ: P,
}

impl<P: PrimitiveType> SubPrimitiveFunction<P> {
    const fn new(typ: P) -> Self {
        SubPrimitiveFunction { typ }
    }
}

impl<P: PrimitiveType> ScalarFunction for SubPrimitiveFunction<P> {
    fn plan(
        &self,
        _inputs: &[LogicalExpression],
        _operator_schema: &TypeSchema,
    ) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(SubPrimitiveImpl { typ: self.typ }))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubPrimitiveImpl<P: PrimitiveType> {
    typ: P,
}

impl<P: PrimitiveType> PlannedScalarFunction for SubPrimitiveImpl<P> {
    fn name(&self) -> &'static str {
        "sub_primitive_impl"
    }

    fn return_type(&self) -> DataType {
        P::DATATYPE.clone()
    }

    fn execute(&self, inputs: &[&Arc<Array>]) -> Result<Array> {
        prim_sub(inputs)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MulPrimitiveFunction<P: PrimitiveType> {
    typ: P,
}

impl<P: PrimitiveType> MulPrimitiveFunction<P> {
    const fn new(typ: P) -> Self {
        MulPrimitiveFunction { typ }
    }
}

impl<P: PrimitiveType> ScalarFunction for MulPrimitiveFunction<P> {
    fn plan(
        &self,
        _inputs: &[LogicalExpression],
        _operator_schema: &TypeSchema,
    ) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(MulPrimitiveImpl { typ: self.typ }))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MulPrimitiveImpl<P: PrimitiveType> {
    typ: P,
}

impl<P: PrimitiveType> PlannedScalarFunction for MulPrimitiveImpl<P> {
    fn name(&self) -> &'static str {
        "mul_primitive_impl"
    }

    fn return_type(&self) -> DataType {
        P::DATATYPE.clone()
    }

    fn execute(&self, inputs: &[&Arc<Array>]) -> Result<Array> {
        prim_mul(inputs)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DivPrimitiveFunction<P: PrimitiveType> {
    typ: P,
}

impl<P: PrimitiveType> DivPrimitiveFunction<P> {
    const fn new(typ: P) -> Self {
        DivPrimitiveFunction { typ }
    }
}

impl<P: PrimitiveType> ScalarFunction for DivPrimitiveFunction<P> {
    fn plan(
        &self,
        _inputs: &[LogicalExpression],
        _operator_schema: &TypeSchema,
    ) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(DivPrimitiveImpl { typ: self.typ }))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DivPrimitiveImpl<P: PrimitiveType> {
    typ: P,
}

impl<P: PrimitiveType> PlannedScalarFunction for DivPrimitiveImpl<P> {
    fn name(&self) -> &'static str {
        "div_primitive_impl"
    }

    fn return_type(&self) -> DataType {
        P::DATATYPE.clone()
    }

    fn execute(&self, inputs: &[&Arc<Array>]) -> Result<Array> {
        prim_div(inputs)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RemPrimitiveFunction<P: PrimitiveType> {
    typ: P,
}

impl<P: PrimitiveType> RemPrimitiveFunction<P> {
    const fn new(typ: P) -> Self {
        RemPrimitiveFunction { typ }
    }
}

impl<P: PrimitiveType> ScalarFunction for RemPrimitiveFunction<P> {
    fn plan(
        &self,
        _inputs: &[LogicalExpression],
        _operator_schema: &TypeSchema,
    ) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(RemPrimitiveImpl { typ: self.typ }))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RemPrimitiveImpl<P: PrimitiveType> {
    typ: P,
}

impl<P: PrimitiveType> PlannedScalarFunction for RemPrimitiveImpl<P> {
    fn name(&self) -> &'static str {
        "rem_primitive_impl"
    }

    fn return_type(&self) -> DataType {
        P::DATATYPE.clone()
    }

    fn execute(&self, inputs: &[&Arc<Array>]) -> Result<Array> {
        prim_rem(inputs)
    }
}

#[cfg(test)]
mod tests {
    // use rayexec_bullet::array::Int32Array;

    // use super::*;

    // #[test]
    // fn add_i32() {
    //     let a = Arc::new(Array::Int32(Int32Array::from_iter([1, 2, 3])));
    //     let b = Arc::new(Array::Int32(Int32Array::from_iter([4, 5, 6])));

    //     let specialized = Add.specialize(&[DataType::Int32, DataType::Int32]).unwrap();

    //     let out = specialized.execute(&[&a, &b]).unwrap();
    //     let expected = Array::Int32(Int32Array::from_iter([5, 7, 9]));

    //     assert_eq!(expected, out);
    // }

    // #[test]
    // fn sub_i32() {
    //     let a = Arc::new(Array::Int32(Int32Array::from_iter([4, 5, 6])));
    //     let b = Arc::new(Array::Int32(Int32Array::from_iter([1, 2, 3])));

    //     let specialized = Sub.specialize(&[DataType::Int32, DataType::Int32]).unwrap();

    //     let out = specialized.execute(&[&a, &b]).unwrap();
    //     let expected = Array::Int32(Int32Array::from_iter([3, 3, 3]));

    //     assert_eq!(expected, out);
    // }

    // #[test]
    // fn div_i32() {
    //     let a = Arc::new(Array::Int32(Int32Array::from_iter([4, 5, 6])));
    //     let b = Arc::new(Array::Int32(Int32Array::from_iter([1, 2, 3])));

    //     let specialized = Div.specialize(&[DataType::Int32, DataType::Int32]).unwrap();

    //     let out = specialized.execute(&[&a, &b]).unwrap();
    //     let expected = Array::Int32(Int32Array::from_iter([4, 2, 2]));

    //     assert_eq!(expected, out);
    // }

    // #[test]
    // fn rem_i32() {
    //     let a = Arc::new(Array::Int32(Int32Array::from_iter([4, 5, 6])));
    //     let b = Arc::new(Array::Int32(Int32Array::from_iter([1, 2, 3])));

    //     let specialized = Rem.specialize(&[DataType::Int32, DataType::Int32]).unwrap();

    //     let out = specialized.execute(&[&a, &b]).unwrap();
    //     let expected = Array::Int32(Int32Array::from_iter([0, 1, 0]));

    //     assert_eq!(expected, out);
    // }

    // #[test]
    // fn mul_i32() {
    //     let a = Arc::new(Array::Int32(Int32Array::from_iter([4, 5, 6])));
    //     let b = Arc::new(Array::Int32(Int32Array::from_iter([1, 2, 3])));

    //     let specialized = Mul.specialize(&[DataType::Int32, DataType::Int32]).unwrap();

    //     let out = specialized.execute(&[&a, &b]).unwrap();
    //     let expected = Array::Int32(Int32Array::from_iter([4, 10, 18]));

    //     assert_eq!(expected, out);
    // }
}
