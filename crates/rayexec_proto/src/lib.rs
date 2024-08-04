pub mod generated;
pub mod packed;
pub mod util_types;

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

// Not using `cfg(test)` here since that would make this not visible outside of
// this crate.
//
// See: <https://github.com/rust-lang/cargo/issues/8379>
pub mod testutil {
    use crate::ProtoConv;
    use std::fmt::Debug;

    /// Assert that a value roundtrips correctly through the conversion to and
    /// from a protobuf value.
    pub fn assert_proto_roundtrip<P: ProtoConv + PartialEq + Debug>(val: P) {
        let proto = val.to_proto().unwrap();
        let got = P::from_proto(proto).unwrap();

        assert_eq!(val, got);
    }
}
