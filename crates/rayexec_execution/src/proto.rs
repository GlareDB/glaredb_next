use rayexec_error::Result;
use rayexec_proto::ProtoConv;

use crate::database::DatabaseContext;

/// Convert types to/from their protobuf representations with access to the
/// database context.
pub trait DatabaseProtoConv: Sized {
    type ProtoType;

    fn to_proto(&self, context: &DatabaseContext) -> Result<Self::ProtoType>;
    fn from_proto(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self>;
}

/// Default implementation for anything implementing the stateless proto
/// conversion trait.
///
/// The database context that's provide is just ignored, and the underlying
/// to/from methods are called.
impl<P: ProtoConv> DatabaseProtoConv for P {
    type ProtoType = P::ProtoType;

    fn to_proto(&self, _context: &DatabaseContext) -> Result<Self::ProtoType> {
        self.to_proto()
    }

    fn from_proto(proto: Self::ProtoType, _context: &DatabaseContext) -> Result<Self> {
        Self::from_proto(proto)
    }
}
