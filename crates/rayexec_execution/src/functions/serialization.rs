use rayexec_error::{RayexecError, Result};
use serde::{
    de::{self, DeserializeSeed, Visitor},
    ser::SerializeStruct,
    Deserialize, Deserializer, Serialize, Serializer,
};
use std::fmt;

use crate::database::{catalog::CatalogTx, DatabaseContext};

use super::scalar::{PlannedScalarFunction, ScalarFunction};

#[derive(Debug, Clone, Copy)]
pub struct DatabaseObjectVistor;

impl<'de> Visitor<'de> for DatabaseObjectVistor {
    type Value = &'de str;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        // This Visitor expects to receive...
        write!(formatter, "a database object identifier")
    }

    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(v)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct WhatVisitor<'a> {
    pub context: &'a DatabaseContext,
}

impl<'a> WhatVisitor<'a> {
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

impl<'a, 'de> Visitor<'de> for WhatVisitor<'a> {
    type Value = &'de str;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        // This Visitor expects to receive...
        write!(formatter, "a database object identifier")
    }

    fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
    where
        A: de::MapAccess<'de>,
    {
        unimplemented!()
    }

    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(v)
    }
}

impl Serialize for dyn ScalarFunction + '_ {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_struct("scalar_function", 1)?;
        s.serialize_field("function_name", self.name())?;
        s.end()
    }
}

impl Serialize for dyn PlannedScalarFunction + '_ {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_struct("planned_scalar_function", 2)?;
        s.serialize_field("function_name", self.scalar_function().name())?;
        s.serialize_field("function_data", self.serializable_data())?;
        s.end()
    }
}

#[derive(Debug)]
pub struct PlannedScalarFunctionDeserializer<'a> {
    pub context: &'a DatabaseContext,
}

impl<'a> PlannedScalarFunctionDeserializer<'a> {
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

impl<'de, 'a> DeserializeSeed<'de> for PlannedScalarFunctionDeserializer<'a> {
    type Value = Box<dyn ScalarFunction>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        let what = deserializer.deserialize_struct(
            "planned_scalar_function",
            &["function_name", "function_data"],
            DatabaseObjectVistor,
        )?;

        unimplemented!()
    }
}

#[derive(Debug)]
pub struct SerializableScalarFunction {
    pub name: &'static str,
    pub func: Box<dyn ScalarFunction>,
}

impl Serialize for SerializableScalarFunction {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.name)
    }
}

#[derive(Debug)]
pub struct ScalarFunctionDeserializer<'a> {
    pub context: &'a DatabaseContext,
}

impl<'a> ScalarFunctionDeserializer<'a> {
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

impl<'de, 'a> DeserializeSeed<'de> for ScalarFunctionDeserializer<'a> {
    type Value = Box<dyn ScalarFunction>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        let name = deserializer.deserialize_str(DatabaseObjectVistor)?;
        let scalar = self.get_scalar_function(name).map_err(de::Error::custom)?;

        Ok(scalar)
    }
}
