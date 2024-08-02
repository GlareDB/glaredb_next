use rayexec_error::Result;

pub mod generated;

/// Convert type to/from their protobuf representations.
pub trait ProtoConv: Sized {
    type ProtoType;

    fn to_proto(&self) -> Result<Self::ProtoType>;
    fn from_proto(proto: Self::ProtoType) -> Result<Self>;
}
