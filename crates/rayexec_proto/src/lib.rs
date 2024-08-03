pub mod generated;
pub mod packed;

use rayexec_error::Result;

/// Convert type to/from their protobuf representations.
///
/// This should be implemented for types that are stateless conversions.
pub trait ProtoConv: Sized {
    /// The type we're converting to/from.
    type ProtoType;

    fn to_proto(&self) -> Result<Self::ProtoType>;
    fn from_proto(proto: Self::ProtoType) -> Result<Self>;
}
