use crate::logical::operator::LogicalExpression;
use rayexec_bullet::{
    array::{Array, BooleanArray, BooleanValuesBuffer},
    datatype::{DataType, DataTypeId},
    executor::scalar::BinaryExecutor,
    field::TypeSchema,
};
use rayexec_error::Result;
use std::{collections::HashMap, marker::PhantomData, sync::Arc};

// pub fn eq_function_set() -> ScalarFunctionSet {
//     let mut functions = HashMap::new();
//     functions.insert(
//         &[DataTypeId::Int8, DataTypeId::Int8],
//         Box::new(EqInt8::new()),
//     );
//     functions.insert(
//         &[DataTypeId::Boolean, DataTypeId::Boolean],
//         Box::new(EqBoolean::new()),
//     );
//     functions.insert(
//         &[DataTypeId::Utf8, DataTypeId::Utf8],
//         Box::new(EqUtf8::new()),
//     );

//     ScalarFunctionSet {
//         name: "eq",
//         functions,
//     }
// }

// pub struct Eq<Type: PartialEq + 'static, A: StaticArrayUnwrap<Type>> {
//     _type: PhantomData<Type>,
//     _array: PhantomData<A>,
// }

// pub type EqUtf8 = Eq<str, Utf8ArrayUnwrap>;
// pub type EqBoolean = Eq<bool, BooleanArrayUnwrap>;
// pub type EqInt8 = Eq<i8, Int8ArrayUnwrap>;

// impl<Type: PartialEq + 'static, A: StaticArrayUnwrap<Type>> Eq<Type, A> {
//     fn new() -> Self {
//         Eq {
//             _type: PhantomData,
//             _array: PhantomData,
//         }
//     }
// }

// impl<Type: PartialEq + 'static, A: StaticArrayUnwrap<Type>> ScalarFunction for Eq<Type, A> {
//     fn plan(
//         &self,
//         _inputs: &[LogicalExpression],
//         _operator_schema: &TypeSchema,
//     ) -> Result<Box<dyn PlannedScalarFunction>> {
//         Ok(Box::new(Self::new()))
//     }
// }

// impl<Type: PartialEq + 'static, A: StaticArrayUnwrap<Type>> PlannedScalarFunction for Eq<Type, A> {
//     fn return_type(&self) -> DataType {
//         DataType::Boolean
//     }

//     fn execute(&self, inputs: &[&Arc<Array>]) -> Result<Array> {
//         let left = A::unwrap_array(&inputs[0]);
//         let right = A::unwrap_array(&inputs[1]);

//         let mut values = BooleanValuesBuffer::default();

//         let validity = BinaryExecutor::execute(left, right, |a, b| a == b, &mut values)?;

//         Ok(Array::Boolean(BooleanArray::new(values, validity)))
//     }
// }
