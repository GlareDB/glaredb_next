use rayexec_error::{OptionExt, Result};
use rayexec_parser::ast;
use rayexec_proto::ProtoConv;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::database::{catalog_entry::CatalogEntry, AttachInfo};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CteIndex(pub usize);

/// Table or CTE found in the FROM clause.
#[derive(Debug, Clone, PartialEq)]
pub enum BoundTableOrCteReference {
    /// Resolved table.
    Table {
        catalog: String,
        schema: String,
        entry: Arc<CatalogEntry>,
    },
    /// Resolved CTE.
    Cte {
        /// Index of the cte in the bind data.
        cte_idx: CteIndex,
    },
}

impl ProtoConv for BoundTableOrCteReference {
    type ProtoType = rayexec_proto::generated::binder::BoundTableOrCteReference;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        // use rayexec_proto::generated::binder::{
        //     bound_table_or_cte_reference::Value, BoundCteReference, BoundTableReference,
        // };

        // let value = match self {
        //     Self::Table {
        //         catalog,
        //         schema,
        //         entry,
        //     } => Value::Table(BoundTableReference {
        //         catalog: catalog.clone(),
        //         schema: schema.clone(),
        //         table: Some(entry.to_proto()?),
        //     }),
        //     Self::Cte { cte_idx } => Value::Cte(BoundCteReference {
        //         idx: cte_idx.0 as u32,
        //     }),
        // };

        // Ok(Self::ProtoType { value: Some(value) })
        unimplemented!()
    }

    fn from_proto(_proto: Self::ProtoType) -> Result<Self> {
        // use rayexec_proto::generated::binder::bound_table_or_cte_reference::Value;

        unimplemented!()
        // Ok(match proto.value.required("value")? {
        //     Value::Table(table) => Self::Table {
        //         catalog: table.catalog,
        //         schema: table.schema,
        //         entry: TableEntry::from_proto(table.table.required("table")?)?,
        //     },
        //     Value::Cte(cte) => Self::Cte {
        //         cte_idx: CteIndex(cte.idx as usize),
        //     },
        // })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct UnboundTableReference {
    /// The raw ast reference.
    pub reference: ast::ObjectReference,
    /// Name of the catalog this table is in.
    pub catalog: String,
    /// How we attach the catalog.
    pub attach_info: Option<AttachInfo>,
}

impl ProtoConv for UnboundTableReference {
    type ProtoType = rayexec_proto::generated::binder::UnboundTableReference;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            reference: Some(self.reference.to_proto()?),
            catalog: self.catalog.clone(),
            attach_info: self
                .attach_info
                .as_ref()
                .map(|i| i.to_proto())
                .transpose()?,
        })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        Ok(Self {
            reference: ProtoConv::from_proto(proto.reference.required("reference")?)?,
            catalog: proto.catalog,
            attach_info: proto
                .attach_info
                .map(|i| ProtoConv::from_proto(i))
                .transpose()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use rayexec_bullet::scalar::OwnedScalarValue;
    use rayexec_proto::testutil::assert_proto_roundtrip;

    use super::*;

    #[test]
    fn roundtrip_unbound_table_reference() {
        let reference = UnboundTableReference {
            reference: ast::ObjectReference::from_strings(["my", "table"]),
            catalog: "catalog".to_string(),
            attach_info: Some(AttachInfo {
                datasource: "snowbricks".to_string(),
                options: [("key".to_string(), OwnedScalarValue::Float32(3.5))].into(),
            }),
        };

        assert_proto_roundtrip(reference);
    }
}
