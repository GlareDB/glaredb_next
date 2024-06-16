use rayexec_bullet::datatype::DataType;

/// Return the score for casting from `have` to `want`.
///
/// A higher score indicates a more preferred cast.
///
/// This is a best-effort attempt to determine if casting from one type to
/// another is valid and won't lose precision.
pub const fn implicit_cast_score(have: &DataType, want: &DataType) -> i32 {
    // Cast NULL to anything.
    if have.is_null() {
        return target_score(want);
    }

    match have {
        // Simple integer casts.
        DataType::Int8 => return int8_cast_score(want),
        DataType::Int16 => return int16_cast_score(want),
        DataType::Int32 => return int32_cast_score(want),
        DataType::Int64 => return int64_cast_score(want),
        DataType::UInt8 => return uint8_cast_score(want),
        DataType::UInt16 => return uint16_cast_score(want),
        DataType::UInt32 => return uint32_cast_score(want),
        DataType::UInt64 => return uint64_cast_score(want),

        // Float casts
        DataType::Float32 => return float32_cast_score(want),
        DataType::Float64 => return float64_cast_score(want),

        // String casts
        DataType::Utf8 | DataType::LargeUtf8 => match want {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Decimal64(_)
            | DataType::Decimal128(_)
            | DataType::Interval
            | DataType::TimestampSeconds
            | DataType::TimestampMilliseconds
            | DataType::TimestampMicroseconds
            | DataType::TimestampNanoseconds => return target_score(want),

            // Non-zero since it's a valid cast, just we would prefer something
            // else.
            DataType::Utf8 | DataType::LargeUtf8 => return 1,
            _ => (),
        },
        _ => (),
    }

    // No valid cast found.
    -1
}

/// Determine the score for the target type we can cast to.
///
/// More "specific" types will have a higher target score.
const fn target_score(target: &DataType) -> i32 {
    match target {
        DataType::Utf8 => 1,
        DataType::Int64 => 101,
        DataType::UInt64 => 102,
        DataType::Int32 => 111,
        DataType::UInt32 => 112,
        DataType::Int16 => 121,
        DataType::UInt16 => 122,
        DataType::Int8 => 131,
        DataType::UInt8 => 132,
        DataType::Float32 => 141,
        DataType::Float64 => 142,
        DataType::Decimal64(_) => 151,
        DataType::Decimal128(_) => 152,
        _ => 100,
    }
}

const fn int8_cast_score(want: &DataType) -> i32 {
    match want {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal64(_)
        | DataType::Decimal128(_) => target_score(want),
        _ => -1,
    }
}

const fn int16_cast_score(want: &DataType) -> i32 {
    match want {
        DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal64(_)
        | DataType::Decimal128(_) => target_score(want),
        _ => -1,
    }
}

const fn int32_cast_score(want: &DataType) -> i32 {
    match want {
        DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal64(_)
        | DataType::Decimal128(_) => target_score(want),
        _ => -1,
    }
}

const fn int64_cast_score(want: &DataType) -> i32 {
    match want {
        DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal64(_)
        | DataType::Decimal128(_) => target_score(want),
        _ => -1,
    }
}

const fn uint8_cast_score(want: &DataType) -> i32 {
    match want {
        DataType::UInt8
        | DataType::UInt16
        | DataType::Int16
        | DataType::UInt32
        | DataType::Int32
        | DataType::UInt64
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal64(_)
        | DataType::Decimal128(_) => target_score(want),
        _ => -1,
    }
}

const fn uint16_cast_score(want: &DataType) -> i32 {
    match want {
        DataType::UInt16
        | DataType::UInt32
        | DataType::Int32
        | DataType::UInt64
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal64(_)
        | DataType::Decimal128(_) => target_score(want),
        _ => -1,
    }
}

const fn uint32_cast_score(want: &DataType) -> i32 {
    match want {
        DataType::UInt32
        | DataType::UInt64
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal64(_)
        | DataType::Decimal128(_) => target_score(want),
        _ => -1,
    }
}

const fn uint64_cast_score(want: &DataType) -> i32 {
    match want {
        DataType::UInt64
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal64(_)
        | DataType::Decimal128(_) => target_score(want),
        _ => -1,
    }
}

const fn float32_cast_score(want: &DataType) -> i32 {
    match want {
        DataType::Float64 | DataType::Decimal64(_) | DataType::Decimal128(_) => target_score(want),
        _ => -1,
    }
}

const fn float64_cast_score(want: &DataType) -> i32 {
    match want {
        DataType::Decimal64(_) | DataType::Decimal128(_) => target_score(want),
        _ => -1,
    }
}

#[cfg(test)]
mod tests {
    use rayexec_bullet::datatype::TypeMeta;

    use super::*;

    #[test]
    fn implicit_cast_from_utf8() {
        assert!(implicit_cast_score(&DataType::Utf8, &DataType::Int32) > 0);
        assert!(implicit_cast_score(&DataType::Utf8, &DataType::TimestampMilliseconds) > 0);
        assert!(implicit_cast_score(&DataType::Utf8, &DataType::Interval) > 0);

        assert!(implicit_cast_score(&DataType::LargeUtf8, &DataType::Int32) > 0);
        assert!(implicit_cast_score(&DataType::LargeUtf8, &DataType::TimestampMilliseconds) > 0);
        assert!(implicit_cast_score(&DataType::LargeUtf8, &DataType::Interval) > 0);
    }

    #[test]
    fn never_implicit_to_utf8() {
        // ...except when we're casting from utf8 utf8
        assert!(implicit_cast_score(&DataType::Int16, &DataType::Utf8) < 0);
        assert!(implicit_cast_score(&DataType::TimestampMilliseconds, &DataType::Utf8) < 0);
    }

    #[test]
    fn integer_casts() {
        // Valid
        assert!(implicit_cast_score(&DataType::Int16, &DataType::Int64) > 0);
        assert!(implicit_cast_score(&DataType::Int16, &DataType::Decimal64(TypeMeta::None)) > 0);
        assert!(implicit_cast_score(&DataType::Int16, &DataType::Float32) > 0);

        // Not valid
        assert!(implicit_cast_score(&DataType::Int16, &DataType::UInt64) < 0);
    }

    #[test]
    fn float_casts() {
        // Valid
        assert!(implicit_cast_score(&DataType::Float64, &DataType::Decimal64(TypeMeta::None)) > 0);

        // Not valid
        assert!(implicit_cast_score(&DataType::Float64, &DataType::Int64) < 0);
    }
}
