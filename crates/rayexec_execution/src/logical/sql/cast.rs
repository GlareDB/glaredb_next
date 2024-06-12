use rayexec_bullet::field::DataType;

use crate::functions::{InputTypes, Signature};

/// Determines if a data type we have can be implicity casted to a data type we
/// want.
///
/// This does not determine if casting can actually succeed, just that we can
/// try and (possibly) fail at runtime.
// TODO: Determine if a conversion is lossy (Int64 -> Int32) and omit.
pub const fn has_implicit_cast(have: &DataType, want: &DataType) -> bool {
    match (have, want) {
        // - Ints can be cast to eachother
        (a, b) if a.is_integer() && b.is_integer() && encompasses_domain(b, a) => true,
        // - Ints can be cast to floats
        (a, b) if a.is_integer() && b.is_float() => true,
        // - Ints can be cast to decimals
        (a, b) if a.is_integer() && b.is_decimal() => true,
        // - Strings can be cast to ints
        // - Strings can be cast to floats
        // - Strings can be cast to decimals
        (a, b) if a.is_string() && b.is_numeric() => true,
        // - Strings can be cast to dates
        // - Strings can be cast to timestamps
        // - Strings can be cast to intervals
        (a, b) if a.is_string() && b.is_temporal() => true,
        (_, _) => false,
    }
}

const fn encompasses_domain(a: &DataType, b: &DataType) -> bool {
    match (a, b) {
        // - Int64 > any signed integer
        (DataType::Int64, b) if b.is_signed_integer() => true,
        // - UInt64 > any unsigned integer
        (DataType::UInt64, b) if b.is_signed_integer() => true,
        // - Decimal > any int or float
        (a, b) if a.is_decimal() && (b.is_float() || b.is_integer()) => true,
        (DataType::Decimal128(_, _), DataType::Decimal64(_, _)) => true,
        (_, _) => false,
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CandidateSignature {
    /// The signature of this candidate.
    pub signature: Signature,

    /// The required data types for this signature.
    pub datatypes: Vec<DataType>,
}

/// Given inputs for a function, try to find candidate signatures that can
/// accomadate them.
pub fn find_candidate_signatures(
    inputs: &[DataType],
    sigs: &[Signature],
) -> Vec<CandidateSignature> {
    let mut candidates = Vec::new();

    for sig in sigs {
        // TODO: What to do for non-exact signatures?
        if let InputTypes::Exact(exact) = &sig.input {
            if exact.len() != inputs.len() {
                continue;
            }

            let can_cast = inputs
                .iter()
                .zip(exact.iter())
                .all(|(have, want)| has_implicit_cast(have, want));

            if can_cast {
                candidates.push(CandidateSignature {
                    signature: sig.clone(), // TODO: We could make signatures static to avoid this clone.
                    datatypes: exact.to_vec(),
                })
            }
        }
    }

    candidates
}
