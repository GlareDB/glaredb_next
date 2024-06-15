use rayexec_bullet::datatype::DataType;

use crate::functions::{InputTypes, Signature};

/// Determines if a data type we have can be implicity casted to a data type we
/// want.
///
/// For strings, this does not determine if casting can actually succeed, just
/// that we can try and (possibly) fail at runtime.
pub const fn has_implicit_cast(have: &DataType, want: &DataType) -> bool {
    false
    // match (have, want) {
    //     // - Ints can be cast to eachother
    //     (a, b) if a.is_integer() && b.is_integer() && encompasses_domain(b, a) => true,
    //     // - Ints can be cast to floats
    //     (a, b) if a.is_integer() && b.is_float() => true,
    //     // - Ints can be cast to decimals
    //     (a, b) if a.is_integer() && b.is_decimal() => true,
    //     // - Strings can be cast to ints
    //     // - Strings can be cast to floats
    //     // - Strings can be cast to decimals
    //     (a, b) if a.is_string() && b.is_numeric() => true,
    //     // - Strings can be cast to dates
    //     // - Strings can be cast to timestamps
    //     // - Strings can be cast to intervals
    //     (a, b) if a.is_string() && b.is_temporal() => true,
    //     (_, _) => false,
    // }
}

const fn encompasses_domain(a: &DataType, b: &DataType) -> bool {
    false
    // match (a, b) {
    //     // - Int64 > any signed integer
    //     (DataType::Int64, b) if b.is_signed_integer() => true,
    //     // - UInt64 > any unsigned integer
    //     (DataType::UInt64, b) if b.is_signed_integer() => true,
    //     // - Decimal > any int or float
    //     (a, b) if a.is_decimal() && (b.is_float() || b.is_integer()) => true,
    //     (DataType::Decimal128(_, _), DataType::Decimal64(_, _)) => true,
    //     (_, _) => false,
    // }
}

#[derive(Debug, Clone, PartialEq)]
pub enum CastType {
    /// Need to cast the type to this one.
    CastTo(DataType),

    /// Casting isn't needed, the original data type works.
    NoCastNeeded,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CandidateSignature {
    /// The index of the input signature that this applies to.
    pub signature_idx: usize,

    /// The required data types for this signature.
    pub datatypes: Vec<CastType>,
}

/// Given inputs for a function, try to find candidate signatures that can
/// accomadate them.
pub fn find_candidate_signatures(
    inputs: &[DataType],
    sigs: &[Signature],
) -> Vec<CandidateSignature> {
    let mut candidates = Vec::new();

    let mut typ_buf = Vec::new();
    for (idx, sig) in sigs.iter().enumerate() {
        // TODO: What to do for non-exact signatures?
        if let InputTypes::Exact(exact) = &sig.input {
            if !compare_and_fill_types(inputs, exact, &mut typ_buf) {
                continue;
            }

            candidates.push(CandidateSignature {
                signature_idx: idx, // TODO: We could make signatures static to avoid this clone.
                datatypes: std::mem::take(&mut typ_buf),
            })
        }
    }

    candidates
}

/// Compare the types we have with the types we want, filling the provided
/// buffer with the cast type.
///
/// Returns true if everything is able to be implicitly cast, false otherwise.
fn compare_and_fill_types(have: &[DataType], want: &[DataType], buf: &mut Vec<CastType>) -> bool {
    if have.len() != want.len() {
        return false;
    }
    buf.clear();

    for (have, want) in have.iter().zip(want.iter()) {
        if have == want {
            buf.push(CastType::NoCastNeeded);
            continue;
        }

        if has_implicit_cast(have, want) {
            buf.push(CastType::CastTo(want.clone()));
            continue;
        }

        return false;
    }

    true
}

// #[cfg(test)]
// mod tests {
//     use crate::functions::ReturnType;

//     use super::*;

//     #[test]
//     fn test_implicit_cast() {
//         // Has implicit
//         assert!(has_implicit_cast(&DataType::Int32, &DataType::Int64));
//         assert!(has_implicit_cast(&DataType::Int32, &DataType::Float32));
//         assert!(has_implicit_cast(&DataType::Utf8, &DataType::Float32));

//         // Doesn't have implicit
//         assert!(!has_implicit_cast(&DataType::Int64, &DataType::Int32));
//         assert!(!has_implicit_cast(&DataType::Float32, &DataType::Int32));
//         assert!(!has_implicit_cast(&DataType::Float32, &DataType::Utf8));
//     }

//     #[test]
//     fn no_cast_needed() {
//         let inputs = &[DataType::Int64];
//         let sigs = &[Signature {
//             input: InputTypes::Exact(&[DataType::Int64]),
//             return_type: ReturnType::Static(DataType::Utf8),
//         }];

//         let candidates = find_candidate_signatures(inputs, sigs);
//         let expected = vec![CandidateSignature {
//             signature_idx: 0,
//             datatypes: vec![CastType::NoCastNeeded],
//         }];

//         assert_eq!(expected, candidates)
//     }

//     #[test]
//     fn no_candidates() {
//         // Trying to cast Int64 -> Int32, invalid

//         let inputs = &[DataType::Int64];
//         let sigs = &[Signature {
//             input: InputTypes::Exact(&[DataType::Int32]),
//             return_type: ReturnType::Static(DataType::Utf8),
//         }];

//         let candidates = find_candidate_signatures(inputs, sigs);
//         assert!(candidates.is_empty())
//     }

//     #[test]
//     fn single_candidate() {
//         // Int32 -> Int64

//         let inputs = &[DataType::Int32];
//         let sigs = &[Signature {
//             input: InputTypes::Exact(&[DataType::Int64]),
//             return_type: ReturnType::Static(DataType::Utf8),
//         }];

//         let candidates = find_candidate_signatures(inputs, sigs);
//         let expected = vec![CandidateSignature {
//             signature_idx: 0,
//             datatypes: vec![CastType::CastTo(DataType::Int64)],
//         }];

//         assert_eq!(expected, candidates)
//     }

//     #[test]
//     fn multiple_candidates() {
//         // Int32 -> Int64
//         // Int32 -> Decimal64
//         // Int32 -> Float32

//         let inputs = &[DataType::Int32];
//         let sigs = &[
//             Signature {
//                 input: InputTypes::Exact(&[DataType::Int64]),
//                 return_type: ReturnType::Static(DataType::Utf8),
//             },
//             // Invalid
//             Signature {
//                 input: InputTypes::Exact(&[DataType::Utf8]),
//                 return_type: ReturnType::Static(DataType::Utf8),
//             },
//             Signature {
//                 input: InputTypes::Exact(&[DataType::Decimal64(18, 9)]),
//                 return_type: ReturnType::Static(DataType::Utf8),
//             },
//             Signature {
//                 input: InputTypes::Exact(&[DataType::Float32]),
//                 return_type: ReturnType::Static(DataType::Utf8),
//             },
//         ];

//         let candidates = find_candidate_signatures(inputs, sigs);
//         let expected = vec![
//             CandidateSignature {
//                 signature_idx: 0,
//                 datatypes: vec![CastType::CastTo(DataType::Int64)],
//             },
//             CandidateSignature {
//                 signature_idx: 2,
//                 datatypes: vec![CastType::CastTo(DataType::Decimal64(18, 9))],
//             },
//             CandidateSignature {
//                 signature_idx: 3,
//                 datatypes: vec![CastType::CastTo(DataType::Float32)],
//             },
//         ];

//         assert_eq!(expected, candidates)
//     }

//     #[test]
//     fn mixed_args_need_casting() {
//         // (Int64, Int32) -> (Int64, Int64)

//         let inputs = &[DataType::Int64, DataType::Int32];
//         let sigs = &[Signature {
//             input: InputTypes::Exact(&[DataType::Int64, DataType::Int64]),
//             return_type: ReturnType::Static(DataType::Utf8),
//         }];

//         let candidates = find_candidate_signatures(inputs, sigs);
//         let expected = vec![CandidateSignature {
//             signature_idx: 0,
//             datatypes: vec![CastType::NoCastNeeded, CastType::CastTo(DataType::Int64)],
//         }];

//         assert_eq!(expected, candidates)
//     }

//     #[test]
//     fn binary_int64_decimal() {
//         // (Int64, Decimal64(15, 2)) -> (Decimal64(15, 2), Decimal64(15, 2))

//         let inputs = &[DataType::Int64, DataType::Decimal64(15, 2)];
//         let sigs = &[Signature {
//             input: InputTypes::Exact(&[DataType::Decimal64(15, 2), DataType::Decimal64(15, 2)]),
//             return_type: ReturnType::Static(DataType::Decimal64(15, 2)),
//         }];

//         let candidates = find_candidate_signatures(inputs, sigs);
//         let expected = vec![CandidateSignature {
//             signature_idx: 0,
//             datatypes: vec![
//                 CastType::CastTo(DataType::Decimal64(15, 2)),
//                 CastType::NoCastNeeded,
//             ],
//         }];

//         assert_eq!(expected, candidates)
//     }
// }
