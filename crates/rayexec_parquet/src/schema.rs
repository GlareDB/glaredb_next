use parquet::{
    basic::{ConvertedType, LogicalType, TimeUnit},
    schema::types::{BasicTypeInfo, SchemaDescriptor, Type},
};
use rayexec_bullet::field::{DataType, Field, Schema};
use rayexec_error::{RayexecError, Result};

/// Converts a parquet schema to a bullet schema.
///
/// A lot of this logic was taken from the conversion to arrow in the upstream
/// arrow-rs crate.
pub fn convert_schema(parquet_schema: &SchemaDescriptor) -> Result<Schema> {
    match parquet_schema.root_schema() {
        Type::GroupType { fields, .. } => {
            let fields = convert_types_to_fields(fields)?;
            Ok(Schema::new(fields))
        }
        Type::PrimitiveType { .. } => unreachable!("schema type is not primitive"),
    }
}

fn convert_complex(parquet_type: &Type) -> Result<DataType> {
    match parquet_type {
        Type::GroupType {
            basic_info,
            fields: _,
        } => {
            match basic_info.converted_type() {
                ConvertedType::LIST => unimplemented!(),
                ConvertedType::MAP | ConvertedType::MAP_KEY_VALUE => unimplemented!(),
                _ => {
                    // let struct_fields = convert_group_fields(parquet_type)?;
                    unimplemented!()
                }
            }
        }
        Type::PrimitiveType { .. } => unreachable!(),
    }
}

fn convert_types_to_fields<T: AsRef<Type>>(typs: &[T]) -> Result<Vec<Field>> {
    let mut fields = Vec::with_capacity(typs.len());

    for parquet_type in typs.iter() {
        let parquet_type = parquet_type.as_ref();
        let dt = if parquet_type.is_primitive() {
            convert_primitive(parquet_type)?
        } else {
            convert_complex(parquet_type)?
        };

        let field = Field::new(parquet_type.name(), dt, true); // TODO: Nullable from repetition.
        fields.push(field);
    }

    Ok(fields)
}

/// Convert a primitive type to a bullet data type.
///
/// <https://github.com/apache/parquet-format/blob/master/LogicalTypes.md>
fn convert_primitive(parquet_type: &Type) -> Result<DataType> {
    match parquet_type {
        Type::PrimitiveType {
            basic_info,
            physical_type,
            type_length: _,
            scale,
            precision,
        } => match physical_type {
            parquet::basic::Type::BOOLEAN => Ok(DataType::Boolean),
            parquet::basic::Type::INT32 => from_int32(basic_info, *scale, *precision),
            parquet::basic::Type::INT64 => from_int64(basic_info, *scale, *precision),
            parquet::basic::Type::INT96 => unimplemented!(),
            parquet::basic::Type::FLOAT => Ok(DataType::Float32),
            parquet::basic::Type::DOUBLE => Ok(DataType::Float64),
            parquet::basic::Type::BYTE_ARRAY => from_byte_array(basic_info, *precision, *scale),
            parquet::basic::Type::FIXED_LEN_BYTE_ARRAY => unimplemented!(),
        },
        Type::GroupType { .. } => unreachable!(),
    }
}

fn from_int32(info: &BasicTypeInfo, scale: i32, precision: i32) -> Result<DataType> {
    match (info.logical_type(), info.converted_type()) {
        (None, ConvertedType::NONE) => Ok(DataType::Int32),
        (
            Some(
                ref t @ LogicalType::Integer {
                    bit_width,
                    is_signed,
                },
            ),
            _,
        ) => match (bit_width, is_signed) {
            (8, true) => Ok(DataType::Int8),
            (16, true) => Ok(DataType::Int16),
            (32, true) => Ok(DataType::Int32),
            (8, false) => Ok(DataType::UInt8),
            (16, false) => Ok(DataType::UInt16),
            (32, false) => Ok(DataType::UInt32),
            _ => Err(RayexecError::new(format!(
                "Cannot create INT32 physical type from {:?}",
                t
            ))),
        },
        (Some(LogicalType::Decimal { scale, precision }), _) => unimplemented!(),
        (Some(LogicalType::Date), _) => unimplemented!(),
        (Some(LogicalType::Time { unit, .. }), _) => match unit {
            TimeUnit::MILLIS(_) => unimplemented!(),
            _ => Err(RayexecError::new(format!(
                "Cannot create INT32 physical type from {:?}",
                unit
            ))),
        },
        // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#unknown-always-null
        (Some(LogicalType::Unknown), _) => Ok(DataType::Null),
        (None, ConvertedType::UINT_8) => Ok(DataType::UInt8),
        (None, ConvertedType::UINT_16) => Ok(DataType::UInt16),
        (None, ConvertedType::UINT_32) => Ok(DataType::UInt32),
        (None, ConvertedType::INT_8) => Ok(DataType::Int8),
        (None, ConvertedType::INT_16) => Ok(DataType::Int16),
        (None, ConvertedType::INT_32) => Ok(DataType::Int32),
        (None, ConvertedType::DATE) => unimplemented!(),
        (None, ConvertedType::TIME_MILLIS) => unimplemented!(),
        (None, ConvertedType::DECIMAL) => unimplemented!(),
        (logical, converted) => Err(RayexecError::new(format!(
            "Unable to convert parquet INT32 logical type {:?} or converted type {}",
            logical, converted
        ))),
    }
}

fn from_int64(info: &BasicTypeInfo, scale: i32, precision: i32) -> Result<DataType> {
    match (info.logical_type(), info.converted_type()) {
        (None, ConvertedType::NONE) => Ok(DataType::Int64),
        (
            Some(LogicalType::Integer {
                bit_width: 64,
                is_signed,
            }),
            _,
        ) => match is_signed {
            true => Ok(DataType::Int64),
            false => Ok(DataType::UInt64),
        },
        (Some(LogicalType::Time { unit, .. }), _) => match unit {
            TimeUnit::MILLIS(_) => Err(RayexecError::new(
                "Cannot create INT64 from MILLIS time unit",
            )),
            TimeUnit::MICROS(_) => unimplemented!(),
            TimeUnit::NANOS(_) => unimplemented!(),
        },
        (
            Some(LogicalType::Timestamp {
                is_adjusted_to_u_t_c,
                unit,
            }),
            _,
        ) => unimplemented!(), // Ok(DataType::Timestamp(
        //     match unit {
        //         TimeUnit::MILLIS(_) => unimplemented!(),
        //         TimeUnit::MICROS(_) => unimplemented!(),
        //         TimeUnit::NANOS(_) => unimplemented!(),
        //     },
        //     if is_adjusted_to_u_t_c {
        //         Some("UTC".into())
        //     } else {
        //         None
        //     },
        // ))
        (None, ConvertedType::INT_64) => Ok(DataType::Int64),
        (None, ConvertedType::UINT_64) => Ok(DataType::UInt64),
        (None, ConvertedType::TIME_MICROS) => unimplemented!(),
        (None, ConvertedType::TIMESTAMP_MILLIS) => unimplemented!(),
        (None, ConvertedType::TIMESTAMP_MICROS) => unimplemented!(),
        (Some(LogicalType::Decimal { scale, precision }), _) => unimplemented!(),
        (None, ConvertedType::DECIMAL) => unimplemented!(),
        (logical, converted) => Err(RayexecError::new(format!(
            "Unable to convert parquet INT64 logical type {:?} or converted type {}",
            logical, converted
        ))),
    }
}

fn from_byte_array(info: &BasicTypeInfo, precision: i32, scale: i32) -> Result<DataType> {
    match (info.logical_type(), info.converted_type()) {
        (Some(LogicalType::String), _) => Ok(DataType::Utf8),
        (Some(LogicalType::Json), _) => Ok(DataType::Utf8),
        (Some(LogicalType::Bson), _) => Ok(DataType::Binary),
        (Some(LogicalType::Enum), _) => Ok(DataType::Binary),
        (None, ConvertedType::NONE) => Ok(DataType::Binary),
        (None, ConvertedType::JSON) => Ok(DataType::Utf8),
        (None, ConvertedType::BSON) => Ok(DataType::Binary),
        (None, ConvertedType::ENUM) => Ok(DataType::Binary),
        (None, ConvertedType::UTF8) => Ok(DataType::Utf8),
        (
            Some(LogicalType::Decimal {
                scale: s,
                precision: p,
            }),
            _,
        ) => unimplemented!(),
        (None, ConvertedType::DECIMAL) => unimplemented!(),
        (logical, converted) => Err(RayexecError::new(format!(
            "Unable to convert parquet BYTE_ARRAY logical type {:?} or converted type {}",
            logical, converted
        ))),
    }
}