//! (De)serialization logic for physical operators.
use crate::database::DatabaseContext;
use serde::{
    de::{DeserializeSeed, MapAccess, Visitor},
    Deserializer, Serialize, Serializer,
};
use std::fmt;
