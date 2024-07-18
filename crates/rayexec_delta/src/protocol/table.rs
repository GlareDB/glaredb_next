use std::sync::Arc;

use futures::StreamExt;
use rayexec_bullet::{
    datatype::{DataType, DecimalTypeMeta, TimeUnit, TimestampTypeMeta},
    field::{Field, Schema},
    scalar::decimal::{Decimal128Type, DecimalType, DECIMAL_DEFUALT_SCALE},
};
use rayexec_error::{not_implemented, Result, ResultExt};
use rayexec_io::{FileLocation, FileProvider};
use serde_json::Deserializer;

use crate::protocol::schema::{PrimitiveType, SchemaType};

use super::{
    action::Action,
    schema::{StructField, StructType},
    snapshot::Snapshot,
};

/// Relative path to delta log files.
const DELTA_LOG_PATH: &'static str = "_delta_log";

#[derive(Debug)]
pub struct Table {
    /// Root of the table.
    root: FileLocation,
    /// Provider for accessing files.
    provider: Arc<dyn FileProvider>,
    /// Snapshot of the table, including what files we have available to use for
    /// reading.
    snapshot: Snapshot,
}

impl Table {
    /// Try to load a table at the given location.
    pub async fn load(root: FileLocation, provider: Arc<dyn FileProvider>) -> Result<Self> {
        // TODO: Actually iterate through the commit log...

        let first = root.join([DELTA_LOG_PATH, "00000000000000000000.json"])?;

        let bytes = provider
            .file_source(first)?
            .read_stream()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        // TODO: Either move this to a utility, or avoid doing it.
        let bytes = bytes.into_iter().fold(Vec::new(), |mut v, buf| {
            v.extend_from_slice(buf.as_ref());
            v
        });

        let actions = Deserializer::from_slice(&bytes)
            .into_iter::<Action>()
            .collect::<Result<Vec<_>, _>>()
            .context("failed to read first commit log")?;

        let snapshot = Snapshot::try_new_from_actions(actions)?;

        Ok(Table {
            root,
            provider,
            snapshot,
        })
    }

    pub fn table_schema(&self) -> Result<Schema> {
        let schema = self.snapshot.schema()?;
        schema_from_struct_type(schema)
    }
}

/// Create a schema from a struct type representing the schema of a delta table.
pub fn schema_from_struct_type(typ: StructType) -> Result<Schema> {
    let fields = typ
        .fields
        .into_iter()
        .map(struct_field_to_field)
        .collect::<Result<Vec<_>>>()?;
    Ok(Schema::new(fields))
}

fn struct_field_to_field(field: StructField) -> Result<Field> {
    let datatype = match field.typ {
        SchemaType::Primitive(prim) => match prim {
            PrimitiveType::String => DataType::Utf8,
            PrimitiveType::Long => DataType::Int64,
            PrimitiveType::Integer => DataType::Int32,
            PrimitiveType::Short => DataType::Int16,
            PrimitiveType::Byte => DataType::Int8,
            PrimitiveType::Float => DataType::Float32,
            PrimitiveType::Double => DataType::Float64,
            PrimitiveType::Decimal => DataType::Decimal128(DecimalTypeMeta::new(
                Decimal128Type::MAX_PRECISION,
                DECIMAL_DEFUALT_SCALE,
            )),
            PrimitiveType::Boolean => DataType::Boolean,
            PrimitiveType::Binary => DataType::Binary,
            PrimitiveType::Date => DataType::Timestamp(TimestampTypeMeta::new(TimeUnit::Second)), // TODO: This is just year/month/day
            PrimitiveType::Timestamp => {
                DataType::Timestamp(TimestampTypeMeta::new(TimeUnit::Microsecond))
            }
        },
        SchemaType::Struct(_) => not_implemented!("delta struct"),
        SchemaType::Array(_) => not_implemented!("delta array"),
        SchemaType::Map(_) => not_implemented!("delta map"),
    };

    Ok(Field::new(field.name, datatype, field.nullable))
}
