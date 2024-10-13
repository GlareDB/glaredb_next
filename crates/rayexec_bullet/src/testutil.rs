//! Utilities useful for testing.
//!
//! Note these aren't placed behind an `cfg[(test)]` annotation since they
//! should be usable outside of the crate.

use crate::array::Array;

/// Asserts that two arrays are logical equal.
///
/// This takes into account selections and validity by just iterating over the
/// returned scalar values.
pub fn assert_arrays_eq(a: &Array, b: &Array) {
    if a.logical_len() != b.logical_len() {
        panic!(
            "Array lengths differ, got {} and {}",
            a.logical_len(),
            b.logical_len()
        );
    }

    let len = a.logical_len();

    for idx in 0..len {
        let a_scalar = a.logical_value(idx).unwrap();
        let b_scalar = b.logical_value(idx).unwrap();

        assert_eq!(a_scalar, b_scalar, "Scalars differ at index {idx}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::selection::SelectionVector;

    #[test]
    fn assert_arrrays_eq_true() {
        let a = Array::from_iter([2, 3, 4]);
        let b = Array::from_iter([2, 3, 4]);
        assert_arrays_eq(&a, &b);
    }

    #[test]
    #[should_panic]
    fn assert_arrrays_eq_array_has_null() {
        let a = Array::from_iter([2, 3, 4]);
        let mut b = Array::from_iter([2, 3, 4]);
        b.set_physical_validity(1, false);
        assert_arrays_eq(&a, &b);
    }

    #[test]
    fn assert_arrrays_eq_array_has_selection() {
        let a = Array::from_iter([2, 2, 2]);
        let mut b = Array::from_iter([2]);
        b.select_mut(&SelectionVector::repeated(3, 0).into());

        assert_arrays_eq(&a, &b);
    }

    #[test]
    #[should_panic]
    fn assert_arrrays_eq_different_lengths() {
        let a = Array::from_iter([2, 3, 4]);
        let b = Array::from_iter([2, 3]);
        assert_arrays_eq(&a, &b);
    }

    #[test]
    #[should_panic]
    fn assert_arrrays_eq_different_values() {
        let a = Array::from_iter([2, 3, 4]);
        let b = Array::from_iter([2, 3, 5]);
        assert_arrays_eq(&a, &b);
    }
}
