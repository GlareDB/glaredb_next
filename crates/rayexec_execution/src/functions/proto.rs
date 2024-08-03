use crate::{
    database::{catalog::CatalogTx, DatabaseContext},
    proto::DatabaseProtoConv,
};
use rayexec_error::{OptionExt, Result};

use super::scalar::{PlannedScalarFunction, ScalarFunction};

impl DatabaseProtoConv for Box<dyn ScalarFunction> {
    type ProtoType = rayexec_proto::generated::expr::ScalarFunction;

    fn to_proto_ctx(&self, _context: &DatabaseContext) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            name: self.name().to_string(),
        })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        let tx = &CatalogTx {};
        let scalar = context
            .system_catalog()?
            .get_scalar_fn(tx, "glare_catalog", &proto.name)?
            .required("scalar function")?;

        Ok(scalar)
    }
}

impl DatabaseProtoConv for Box<dyn PlannedScalarFunction> {
    type ProtoType = rayexec_proto::generated::expr::PlannedScalarFunction;

    fn to_proto_ctx(&self, _context: &DatabaseContext) -> Result<Self::ProtoType> {
        let mut state = Vec::new();
        self.encode_state(&mut state)?;

        Ok(Self::ProtoType {
            name: self.scalar_function().name().to_string(),
            state,
        })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        let tx = &CatalogTx {};
        let scalar = context
            .system_catalog()?
            .get_scalar_fn(tx, "glare_catalog", &proto.name)?
            .required("scalar function")?;

        let planned = scalar.decode_state(&proto.state)?;

        Ok(planned)
    }
}
