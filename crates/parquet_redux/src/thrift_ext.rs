use thrift::protocol::{TInputProtocol, TOutputProtocol};

/// Reads and writes the struct to Thrift protocols.
///
/// Unlike [`thrift::protocol::TSerializable`] this uses generics instead of trait objects
pub trait TSerializable: Sized {
    fn read_from_in_protocol<T: TInputProtocol>(i_prot: &mut T) -> thrift::Result<Self>;
    fn write_to_out_protocol<T: TOutputProtocol>(&self, o_prot: &mut T) -> thrift::Result<()>;
}
