pub mod aggregate;
pub mod implicit;
pub mod scalar;
pub mod table;

use fmtutil::IntoDisplayableSlice;
use implicit::implicit_cast_score;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_error::{RayexecError, Result};

/// Function signature.
#[derive(Debug, Clone, PartialEq)]
pub struct Signature {
    /// Expected input types for this signature.
    ///
    /// If the last data type is a list, this signature will be considered
    /// variadic.
    pub input: &'static [DataTypeId],

    /// Type of the variadic args if this function is variadic.
    ///
    /// If None, the function is not considered variadic.
    pub variadic: Option<DataTypeId>,

    /// The expected return type.
    ///
    /// This is purely informational (and could be used for documentation). The
    /// concrete data type is determined by the planned function, which is what
    /// gets used during planning.
    pub return_type: DataTypeId,
}

impl Signature {
    /// Check if this signature is a variadic signature.
    pub const fn is_variadic(&self) -> bool {
        self.variadic.is_some()
    }

    /// Return if inputs given data types exactly satisfy the signature.
    fn exact_match(&self, inputs: &[DataType]) -> bool {
        if self.input.len() != inputs.len() && !self.is_variadic() {
            return false;
        }

        for (&expected, have) in self.input.iter().zip(inputs.iter()) {
            if expected == DataTypeId::Any {
                continue;
            }

            if have.datatype_id() != expected {
                return false;
            }
        }

        // Check variadic.
        if let Some(expected) = self.variadic {
            let remaining = &inputs[self.input.len()..];
            for have in remaining {
                if have.datatype_id() != expected {
                    return false;
                }
            }
        }

        true
    }
}

/// Trait for defining informating about functions.
pub trait FunctionInfo {
    /// Name of the function.
    fn name(&self) -> &'static str;

    /// Aliases for the function.
    ///
    /// When the system catalog is initialized, the function will be placed into
    /// the catalog using both its name and all of its aliases.
    fn aliases(&self) -> &'static [&'static str] {
        &[]
    }

    /// Signature for the function.
    ///
    /// This is used during binding/planning to determine the return type for a
    /// function given some inputs, and how we should handle implicit casting.
    fn signatures(&self) -> &[Signature];

    /// Get the signature for a function if it's an exact match for the inputs.
    ///
    /// If there are no exact signatures for these types, None will be retuned.
    fn exact_signature(&self, inputs: &[DataType]) -> Option<&Signature> {
        self.signatures().iter().find(|sig| sig.exact_match(inputs))
    }

    /// Get candidate signatures for this function given the input datatypes.
    ///
    /// The returned candidates will have info on which arguments need to be
    /// casted and which are fine to state as-is.
    fn candidate(&self, inputs: &[DataType]) -> Vec<CandidateSignature> {
        CandidateSignature::find_candidates(inputs, self.signatures())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum CastType {
    /// Need to cast the type to this one.
    Cast { to: DataTypeId, score: i32 },

    /// Casting isn't needed, the original data type works.
    NoCastNeeded,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CandidateSignature {
    /// Index of the signature
    pub signature_idx: usize,

    /// Casts that would need to be applied in order to satisfy the signature.
    pub casts: Vec<CastType>,
}

impl CandidateSignature {
    /// Find candidate signatures for the given dataypes.
    fn find_candidates(inputs: &[DataType], sigs: &[Signature]) -> Vec<Self> {
        let mut candidates = Vec::new();

        let mut buf = Vec::new();
        for (idx, sig) in sigs.iter().enumerate() {
            if !Self::compare_and_fill_types(inputs, sig.input, sig.variadic, &mut buf) {
                continue;
            }

            candidates.push(CandidateSignature {
                signature_idx: idx,
                casts: std::mem::take(&mut buf),
            })
        }

        candidates
    }

    /// Compare the types we have with the types we want, filling the provided
    /// buffer with the cast type.
    ///
    /// Returns true if everything is able to be implicitly cast, false otherwise.
    fn compare_and_fill_types(
        have: &[DataType],
        want: &[DataTypeId],
        variadic: Option<DataTypeId>,
        buf: &mut Vec<CastType>,
    ) -> bool {
        if have.len() != want.len() && variadic.is_none() {
            return false;
        }
        buf.clear();

        for (have, &want) in have.iter().zip(want.iter()) {
            if have.datatype_id() == want {
                buf.push(CastType::NoCastNeeded);
                continue;
            }

            let score = implicit_cast_score(have, want);
            if score > 0 {
                buf.push(CastType::Cast { to: want, score });
                continue;
            }

            return false;
        }

        // Check variadic.
        if let Some(expected) = variadic {
            let remaining = &have[want.len()..];
            for have in remaining {
                if have.datatype_id() == expected {
                    buf.push(CastType::NoCastNeeded);
                    continue;
                }

                let score = implicit_cast_score(have, expected);
                if score > 0 {
                    buf.push(CastType::Cast {
                        to: expected,
                        score,
                    });
                    continue;
                }

                return false;
            }
        }

        true
    }
}

/// Check the number of arguments provided, erroring if it doesn't match the
/// expected number of arguments.
pub fn plan_check_num_args<T>(
    func: &impl FunctionInfo,
    inputs: &[T],
    expected: usize,
) -> Result<()> {
    if inputs.len() != expected {
        return Err(RayexecError::new(format!(
            "Expected {} input for '{}', received {}",
            expected,
            func.name(),
            inputs.len(),
        )));
    }
    Ok(())
}

/// Return an error indicating the input types we got are not ones we can
/// handle.
// TODO: Include valid signatures in the error
pub fn invalid_input_types_error(func: &impl FunctionInfo, got: &[&DataType]) -> RayexecError {
    RayexecError::new(format!(
        "Got invalid type(s) '{}' for '{}'",
        got.displayable(),
        func.name()
    ))
}
