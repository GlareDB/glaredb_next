use super::{Array, ArrayAccessor};

pub trait ArrayUnwrap<'a, T: ?Sized> {
    fn unwrap_array(array: &'a Array) -> impl ArrayAccessor<T> + 'a;
}

pub struct BooleanArrayUnwrap;

impl<'a> ArrayUnwrap<'a, bool> for BooleanArrayUnwrap {
    fn unwrap_array(array: &'a Array) -> impl ArrayAccessor<bool> + 'a {
        match array {
            Array::Boolean(arr) => arr,
            _ => panic!(),
        }
    }
}

pub struct Int8ArrayUnwrap;

impl<'a> ArrayUnwrap<'a, i8> for Int8ArrayUnwrap {
    fn unwrap_array(array: &'a Array) -> impl ArrayAccessor<i8> + 'a {
        match array {
            Array::Int8(arr) => arr,
            _ => panic!(),
        }
    }
}

pub struct Utf8ArrayUnwrap;

impl<'a> ArrayUnwrap<'a, &'a str> for Utf8ArrayUnwrap {
    fn unwrap_array(array: &'a Array) -> impl ArrayAccessor<&'a str> + 'a {
        match array {
            Array::Utf8(arr) => arr,
            _ => panic!(),
        }
    }
}
