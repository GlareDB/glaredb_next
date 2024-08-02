//! Serde helpers that are generally useful across multiple objects.
//!
//! Object-specific helpers should be defined closer to the objects themselves.
use std::{fmt, marker::PhantomData};

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

pub trait SeqContextVisitor<'de> {
    type Value;

    fn visit_seq_context<A: SeqAccess<'de>>(
        self,
        seq: A,
        context: &DatabaseContext,
    ) -> Result<Self::Value, A::Error>;
}

pub struct SeqContextVisitorWrapper<'a, V> {
    pub context: &'a DatabaseContext,
    pub visitor: V,
}

impl<'a, V> SeqContextVisitorWrapper<'a, V> {
    pub fn new(context: &'a DatabaseContext, visitor: V) -> Self {
        SeqContextVisitorWrapper { context, visitor }
    }
}

impl<'a, 'de, V: SeqContextVisitor<'de>> Visitor<'de> for SeqContextVisitorWrapper<'a, V> {
    type Value = V::Value;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "a sequence representing Value")
    }

    fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        self.visitor.visit_seq_context(seq, self.context)
    }
}
