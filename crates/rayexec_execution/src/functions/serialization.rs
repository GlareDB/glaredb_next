use rayexec_error::{RayexecError, Result};
use serde::{
    de::{self, DeserializeSeed, MapAccess, Visitor},
    ser::SerializeStruct,
    Deserialize, Deserializer, Serialize, Serializer,
};
use std::fmt;

use crate::database::{catalog::CatalogTx, DatabaseContext};

use super::scalar::{PlannedScalarFunction, ScalarFunction};

pub struct DatabaseLookupVisitor<'a> {
    context: &'a DatabaseContext,
}

impl<'a> DatabaseLookupVisitor<'a> {
    fn get_scalar_function(&self, name: &str) -> Result<Box<dyn ScalarFunction>> {
        let tx = CatalogTx::new();
        let func = self
            .context
            .system_catalog()?
            .get_scalar_fn(&tx, "glare_catalog", name)?
            .ok_or_else(|| RayexecError::new(format!("Missing function for '{name}'")))?;

        Ok(func)
    }
}

impl<'de, 'a> Visitor<'de> for DatabaseLookupVisitor<'a> {
    type Value = Box<dyn ScalarFunction>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        // This Visitor expects to receive ...
        write!(formatter, "a database object identifier")
    }

    fn visit_str<E>(self, name: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.get_scalar_function(name)
            .map_err(serde::de::Error::custom)
    }
}

impl<'de, 'a> DeserializeSeed<'de> for DatabaseLookupVisitor<'a> {
    type Value = Box<dyn ScalarFunction>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(self)
    }
}

struct TaggedVisitor<'a> {
    object_name: &'a str,
    context: &'a DatabaseContext,
}

impl<'de, 'a> Visitor<'de> for TaggedVisitor<'a> {
    type Value = Box<dyn PlannedScalarFunction>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "rabbit")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let lookup = DatabaseLookupVisitor {
            context: self.context,
        };

        let scalar = match map.next_key_seed(lookup)? {
            Some(scalar) => scalar,
            None => {
                return Err(de::Error::custom("missing key"));
            }
        };

        map.next_value_seed(FnApply { scalar })
    }
}

pub struct FnApply {
    scalar: Box<dyn ScalarFunction>,
}

impl<'de> DeserializeSeed<'de> for FnApply {
    type Value = Box<dyn PlannedScalarFunction>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mut erased = <dyn erased_serde::Deserializer>::erase(deserializer);
        self.scalar
            .planned_from_deserializer(&mut erased)
            .map_err(serde::de::Error::custom)
    }
}
