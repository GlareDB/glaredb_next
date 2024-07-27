//! Conversion to/from ipc schema.
use super::{
    gen::schema::{
        Field as IpcField, Precision as IpcPrecision, Schema as IpcSchema, Type as IpcType,
    },
    IpcConfig,
};
use crate::{
    datatype::DataType,
    field::{Field, Schema},
};
use rayexec_error::{RayexecError, Result};

pub fn ipc_to_schema(schema: IpcSchema, conf: &IpcConfig) -> Result<Schema> {
    let ipc_fields = schema.fields().unwrap();
    let fields = ipc_fields
        .into_iter()
        .map(|f| ipc_to_field(f, conf))
        .collect::<Result<Vec<_>>>()?;

    Ok(Schema::new(fields))
}

/// Convert an arrow ipc field to a rayexec field..
pub fn ipc_to_field(field: IpcField, conf: &IpcConfig) -> Result<Field> {
    if field.custom_metadata().is_some() {
        // I don't think we'll ever want to support custom metadata, but maybe
        // we should just ignore it.
        return Err(RayexecError::new("metadata unsupported"));
    }

    if field.dictionary().is_some() {
        // TODO
        return Err(RayexecError::new("dictionaries unsupported"));
    }

    let datatype = match field.type_type() {
        IpcType::Null => DataType::Null,
        IpcType::Bool => DataType::Boolean,
        IpcType::Int => {
            let int_type = field.type__as_int().unwrap();
            if int_type.is_signed() {
                match int_type.bitWidth() {
                    8 => DataType::Int8,
                    16 => DataType::Int16,
                    32 => DataType::Int32,
                    64 => DataType::Int64,
                    other => {
                        return Err(RayexecError::new(format!("Unsupported int size: {other}")))
                    }
                }
            } else {
                match int_type.bitWidth() {
                    8 => DataType::UInt8,
                    16 => DataType::UInt16,
                    32 => DataType::UInt32,
                    64 => DataType::UInt64,
                    other => {
                        return Err(RayexecError::new(format!("Unsupported int size: {other}")))
                    }
                }
            }
        }
        IpcType::FloatingPoint => {
            let float_type = field.type__as_floating_point().unwrap();
            match float_type.precision() {
                IpcPrecision::SINGLE => DataType::Float32,
                IpcPrecision::DOUBLE => DataType::Float64,
                other => {
                    return Err(RayexecError::new(format!(
                        "Unsupported float precision: {:?}",
                        other.variant_name()
                    )))
                }
            }
        }
        IpcType::Utf8 => DataType::Utf8,
        IpcType::LargeUtf8 => DataType::LargeUtf8,
        IpcType::Binary => DataType::Binary,
        IpcType::LargeBinary => DataType::LargeBinary,
        other => {
            return Err(RayexecError::new(format!(
                "Unsupported ipc type: {:?}",
                other.variant_name(),
            )))
        }
    };

    Ok(Field {
        name: field.name().unwrap().to_string(),
        datatype,
        nullable: field.nullable(),
    })
}
