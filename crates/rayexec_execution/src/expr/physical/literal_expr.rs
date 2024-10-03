use std::fmt;
use std::sync::Arc;

use rayexec_bullet::{
    array::{Array, Array2},
    batch::Batch,
    bitmap::Bitmap,
    scalar::OwnedScalarValue,
};
use rayexec_error::{OptionExt, Result};
use rayexec_proto::ProtoConv;

use crate::{database::DatabaseContext, proto::DatabaseProtoConv};

#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalLiteralExpr {
    pub literal: OwnedScalarValue,
}

impl PhysicalLiteralExpr {
    pub fn eval(&self, batch: &Batch) -> Result<Array> {
        self.literal.as_array(batch.num_rows())
    }

    pub fn eval2(&self, batch: &Batch, selection: Option<&Bitmap>) -> Result<Arc<Array2>> {
        match selection {
            Some(selection) => Ok(Arc::new(self.literal.as_array2(selection.count_trues()))),
            None => Ok(Arc::new(self.literal.as_array2(batch.num_rows()))),
        }
    }
}

impl fmt::Display for PhysicalLiteralExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.literal)
    }
}

impl DatabaseProtoConv for PhysicalLiteralExpr {
    type ProtoType = rayexec_proto::generated::physical_expr::PhysicalLiteralExpr;

    fn to_proto_ctx(&self, _context: &DatabaseContext) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            literal: Some(self.literal.to_proto()?),
        })
    }

    fn from_proto_ctx(proto: Self::ProtoType, _context: &DatabaseContext) -> Result<Self> {
        Ok(Self {
            literal: ProtoConv::from_proto(proto.literal.required("literal")?)?,
        })
    }
}
