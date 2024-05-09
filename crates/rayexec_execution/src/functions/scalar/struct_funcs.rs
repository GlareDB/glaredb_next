use super::{
    specialize_check_num_args, specialize_invalid_input_type, GenericScalarFunction, InputTypes,
    ReturnType, ScalarFn, Signature, SpecializedScalarFunction,
};
use rayexec_bullet::array::BooleanArrayBuilder;
use rayexec_bullet::executor::BinaryExecutor;
use rayexec_bullet::{array::Array, field::DataType};
use rayexec_error::Result;
use std::fmt::Debug;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StructPack;

impl GenericScalarFunction for StructPack {
    fn name(&self) -> &str {
        "struct_pack"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: InputTypes::Dynamic,
            return_type: ReturnType::Dynamic,
        }]
    }

    fn return_type_for_inputs(&self, inputs: &[DataType]) -> Option<DataType> {
        // TODO: Check "key" types.

        let value_types = inputs.iter().skip(1).step_by(2).cloned().collect();
        Some(DataType::Struct {
            fields: value_types,
        })
    }

    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        unimplemented!()
    }
}
