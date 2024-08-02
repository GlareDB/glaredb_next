//! Serde helpers that are generally useful across multiple objects.
//!
//! Object-specific helpers should be defined closer to the objects themselves.
use std::fmt;

use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};

use crate::database::DatabaseContext;
use rayexec_error::Result;

/// Trait for returning a missing field error from an Option.
pub trait SerdeMissingField<T> {
    fn missing_field<E: de::Error>(self, field: &'static str) -> Result<T, E>;
}

impl<T> SerdeMissingField<T> for Option<T> {
    fn missing_field<E: de::Error>(self, field: &'static str) -> Result<T, E> {
        match self {
            Some(t) => Ok(t),
            None => Err(de::Error::missing_field(field)),
        }
    }
}

/// Helper for deserializing from a map.
pub trait ContextMapDeserialize<'de> {
    type Value: Sized;

    fn deserialize_map<V: MapAccess<'de>>(
        self,
        map: V,
        context: &DatabaseContext,
    ) -> Result<Self::Value, V::Error>;
}

pub struct ContextMapDeserializer<'a, M> {
    pub context: &'a DatabaseContext,
    pub deserializer: M,
}

impl<'a, M> ContextMapDeserializer<'a, M> {
    pub fn new(context: &'a DatabaseContext, deserializer: M) -> Self {
        ContextMapDeserializer {
            context,
            deserializer,
        }
    }
}

impl<'de, 'a, M: ContextMapDeserialize<'de>> Visitor<'de> for ContextMapDeserializer<'a, M> {
    type Value = M::Value;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "a map")
    }

    fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        self.deserializer.deserialize_map(map, self.context)
    }
}

impl<'de, 'a, M: ContextMapDeserialize<'de>> DeserializeSeed<'de>
    for ContextMapDeserializer<'a, M>
{
    type Value = M::Value;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

pub struct ContextSeqDeserializer<'a, M> {
    pub context: &'a DatabaseContext,
    pub deserializer: M,
}

impl<'a, M> ContextSeqDeserializer<'a, M> {
    pub fn new(context: &'a DatabaseContext, deserializer: M) -> Self {
        ContextSeqDeserializer {
            context,
            deserializer,
        }
    }
}

/// Helper for deserializing from a sequence.
pub trait ContextSeqDeserialize<'de> {
    type Value: Sized;

    fn deserialize_seq<V: SeqAccess<'de>>(
        self,
        seq: V,
        context: &DatabaseContext,
    ) -> Result<Self::Value, V::Error>;
}

impl<'de, 'a, M: ContextSeqDeserialize<'de>> Visitor<'de> for ContextSeqDeserializer<'a, M> {
    type Value = M::Value;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "a sequence")
    }

    fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        self.deserializer.deserialize_seq(seq, self.context)
    }
}

impl<'de, 'a, M: ContextSeqDeserialize<'de>> DeserializeSeed<'de>
    for ContextSeqDeserializer<'a, M>
{
    type Value = M::Value;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}
