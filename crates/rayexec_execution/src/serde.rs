use std::fmt;
use std::marker::PhantomData;

use serde::{
    de::{self, DeserializeSeed, MapAccess, Visitor},
    Serialize, Serializer,
};

use crate::database::DatabaseContext;
use rayexec_error::{RayexecError, Result, ResultExt};

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

pub trait ObjectLookup: Copy + 'static {
    /// Object we're looking up that exists in the catalog.
    type Object;

    /// Lookup an object by name in the catalog.
    fn lookup(&self, context: &DatabaseContext, name: &str) -> Result<Self::Object>;
}

pub struct ObjectLookupVisitor<'a, T: ObjectLookup> {
    pub context: &'a DatabaseContext,
    pub lookup: T,
}

impl<'de, 'a, T: ObjectLookup> Visitor<'de> for ObjectLookupVisitor<'a, T> {
    type Value = T::Object;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "object name")
    }

    fn visit_str<E>(self, name: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.lookup
            .lookup(self.context, name)
            .map_err(de::Error::custom)
    }
}

pub trait ContextMapDeserialize<'de>: Sized {
    fn deserialize_map<V: MapAccess<'de>>(
        map: V,
        context: &DatabaseContext,
    ) -> Result<Self, V::Error>;
}

pub struct ContextMapDeserializer<'a, M> {
    pub context: &'a DatabaseContext,
    _visit: PhantomData<M>,
}

impl<'a, M> ContextMapDeserializer<'a, M> {
    pub fn new(context: &'a DatabaseContext) -> Self {
        ContextMapDeserializer {
            context,
            _visit: PhantomData,
        }
    }
}

impl<'de, 'a, M: ContextMapDeserialize<'de>> Visitor<'de> for ContextMapDeserializer<'a, M> {
    type Value = M;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "a map")
    }

    fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        M::deserialize_map(map, self.context)
    }
}

impl<'de, 'a, M: ContextMapDeserialize<'de>> DeserializeSeed<'de>
    for ContextMapDeserializer<'a, M>
{
    type Value = M;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}
