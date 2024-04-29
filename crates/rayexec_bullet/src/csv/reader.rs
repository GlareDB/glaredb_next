//! # Inference
//!
//! Steps:
//!
//! - Infer column delimiters, number of fields per record
//!
//! Can probably use the `Decoder` with differently configured csv readers that
//! repeatedly called on a small sample until we get a configuration that looks
//! reasonable (consistent number of fields across all records in the sample).
//!
//! - Infer types
//!
//! Try to parse into candidate types, starting at the second record in the
//! sample.
//!
//! - Header inferrence
//!
//! Determine if there's a header by trying to parse the first record into the
//! inferred types from the previous step. If it differs, assume a header.

use std::str::FromStr;

use super::decode::{DecodedRecords, Decoder};
use crate::{
    array::{Array, BooleanArray, PrimitiveArray, Utf8Array},
    bitmap::Bitmap,
    field::{DataType, Field},
};
use rayexec_error::{RayexecError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DialectOptions {
    /// Delimiter character.
    pub delimiter: u8,

    /// Quote character.
    pub quote: u8,
}

impl Default for DialectOptions {
    fn default() -> Self {
        DialectOptions {
            delimiter: b',',
            quote: b'"',
        }
    }
}

impl DialectOptions {
    /// Try to infer which csv options to use based on some number of records
    /// from a csv source.
    pub fn infer_from_sample(sample_bytes: &[u8]) -> Result<Self> {
        // Dialect alongside number of fields decoded.
        let mut best: (Option<Self>, usize) = (None, 0);

        for dialect in Self::dialects() {
            let reader = csv_core::ReaderBuilder::new()
                .delimiter(dialect.delimiter)
                .quote(dialect.quote)
                .build();
            let mut decoder = Decoder::new(reader, None);

            match decoder.decode(sample_bytes) {
                Ok(r) => {
                    let decoded_fields = decoder.num_fields().unwrap_or(0);

                    // To be considered the best dialect:
                    //
                    // - Should decode at least 2 records.
                    // - Should read the entirety of the input.
                    // - Should have decoded more number of fields than previous best.
                    if r.completed >= 2
                        && r.input_offset == sample_bytes.len()
                        && decoded_fields > best.1
                    {
                        best = (Some(*dialect), decoded_fields)
                    }

                    // Don't have enough info, try next dialect.
                }
                Err(_e) => {
                    // Assume all errors indicate inconsistent number of fields
                    // in record.
                    //
                    // Continue to next dialect.
                }
            }
        }

        match best.0 {
            Some(best) => Ok(best),
            None => Err(RayexecError::new(
                "Unable to infer csv dialect from provided sample",
            )),
        }
    }

    const fn dialects() -> &'static [Self] {
        &[
            DialectOptions {
                delimiter: b',',
                quote: b'"',
            },
            DialectOptions {
                delimiter: b'|',
                quote: b'"',
            },
            DialectOptions {
                delimiter: b';',
                quote: b'"',
            },
            DialectOptions {
                delimiter: b'\t',
                quote: b'"',
            },
            DialectOptions {
                delimiter: b',',
                quote: b'\'',
            },
            DialectOptions {
                delimiter: b'|',
                quote: b'\'',
            },
            DialectOptions {
                delimiter: b';',
                quote: b'\'',
            },
            DialectOptions {
                delimiter: b'\t',
                quote: b'\'',
            },
        ]
    }
}

#[derive(Debug)]
pub struct TypedDecoder {
    projection: Vec<usize>,

    /// Types to convert fields to.
    types: Vec<DataType>,

    /// Underlying decoder.
    decoder: Decoder,
}

impl TypedDecoder {
    pub fn new(types: Vec<DataType>, decoder: Decoder) -> Self {
        TypedDecoder {
            projection: (0..types.len()).collect(),
            types,
            decoder,
        }
    }

    pub fn decode(&mut self, input: &[u8]) -> Result<usize> {
        let result = self.decoder.decode(input)?;
        // TODO: Double check me.
        assert_eq!(result.input_offset, input.len());
        Ok(result.completed)
    }

    /// Flush out all records into arrays.
    ///
    /// `skip_records` indicates how many records to skip at the beginning. The
    /// skipped records will not be parsed.
    pub fn flush_skip(&mut self, skip_records: usize) -> Result<Vec<Array>> {
        let records = self.decoder.flush()?;

        let array = self
            .projection
            .iter()
            .map(|idx| {
                let typ = &self.types[*idx];
                Ok(match typ {
                    DataType::Boolean => {
                        let mut bits = Bitmap::default();
                        for record in records.iter().skip(skip_records) {
                            // TODO: Nulls
                            let field = record.get_field(*idx)?;
                            let b: bool = field.parse().map_err(|_e| {
                                RayexecError::new(format!("Failed to parse '{field}' into a bool"))
                            })?;
                            bits.push(b);
                        }

                        Array::Boolean(BooleanArray::new_with_values(bits))
                    }
                    DataType::Int8 => {
                        Array::Int8(Self::build_primitive(&records, *idx, skip_records)?)
                    }
                    DataType::Int16 => {
                        Array::Int16(Self::build_primitive(&records, *idx, skip_records)?)
                    }
                    DataType::Int32 => {
                        Array::Int32(Self::build_primitive(&records, *idx, skip_records)?)
                    }
                    DataType::Int64 => {
                        Array::Int64(Self::build_primitive(&records, *idx, skip_records)?)
                    }
                    DataType::UInt8 => {
                        Array::UInt8(Self::build_primitive(&records, *idx, skip_records)?)
                    }
                    DataType::UInt16 => {
                        Array::UInt16(Self::build_primitive(&records, *idx, skip_records)?)
                    }
                    DataType::UInt32 => {
                        Array::UInt32(Self::build_primitive(&records, *idx, skip_records)?)
                    }
                    DataType::UInt64 => {
                        Array::UInt64(Self::build_primitive(&records, *idx, skip_records)?)
                    }
                    DataType::Float32 => {
                        Array::Float32(Self::build_primitive(&records, *idx, skip_records)?)
                    }
                    DataType::Float64 => {
                        Array::Float64(Self::build_primitive(&records, *idx, skip_records)?)
                    }
                    DataType::Utf8 => {
                        // TODO: Nulls
                        let iter = records.iter().skip(skip_records).map(|record| {
                            record.get_field(*idx).unwrap() // TODO: Handle error
                        });
                        Array::Utf8(Utf8Array::from_iter(iter))
                    }
                    DataType::LargeUtf8 => {
                        // TODO: Nulls
                        let iter = records.iter().skip(skip_records).map(|record| {
                            record.get_field(*idx).unwrap() // TODO: Handle error
                        });
                        Array::Utf8(Utf8Array::from_iter(iter))
                    }
                    other => {
                        return Err(RayexecError::new(format!("Unhandled data type: {other:?}")))
                    }
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(array)
    }

    fn build_primitive<T: FromStr>(
        records: &DecodedRecords,
        field: usize,
        skip: usize,
    ) -> Result<PrimitiveArray<T>> {
        let mut values = Vec::with_capacity(records.num_records());

        for record in records.iter().skip(skip) {
            // TODO: Nulls
            let field = record.get_field(field)?;
            let val: T = field
                .parse()
                .map_err(|_e| RayexecError::new(format!("failed to parse '{field}'")))?;
            values.push(val);
        }

        Ok(PrimitiveArray::from(values))
    }
}

#[cfg(test)]
mod tests {
    use crate::array::{Float64Array, Int64Array};

    use super::*;

    #[test]
    fn dialect_infer_ok() {
        struct TestCase {
            csv: String,
            expected: DialectOptions,
        }

        let test_cases = [
            // Simple
            TestCase {
                csv: [
                    "a,b,c", //
                    "d,f,g", //
                    "h,i,j",
                ]
                .join("\n"),
                expected: DialectOptions {
                    delimiter: b',',
                    quote: b'"',
                },
            },
            // Quotes (")
            TestCase {
                csv: [
                    "a,b,c",                //
                    "d,\"hello, world\",g", //
                    "h,i,j",
                ]
                .join("\n"),
                expected: DialectOptions {
                    delimiter: b',',
                    quote: b'"',
                },
            },
            // Alt delimiter
            TestCase {
                csv: [
                    "a|b|c", //
                    "d|f|g", //
                    "h|i|j",
                ]
                .join("\n"),
                expected: DialectOptions {
                    delimiter: b'|',
                    quote: b'"',
                },
            },
            // Quotes (') (note ambiguous)
            TestCase {
                csv: [
                    "a,b,c",             //
                    "d,'hello world',g", //
                    "h,i,j",
                ]
                .join("\n"),
                expected: DialectOptions {
                    delimiter: b',',
                    quote: b'"',
                },
            },
            // Quotes (')
            TestCase {
                csv: [
                    "a,b,c",              //
                    "d,'hello, world',g", //
                    "h,i,j",
                ]
                .join("\n"),
                expected: DialectOptions {
                    delimiter: b',',
                    quote: b'\'',
                },
            },
            // Partial record, last line cut off.
            TestCase {
                csv: "a,b,c\nd,e,f\ng,".to_string(),
                expected: DialectOptions {
                    delimiter: b',',
                    quote: b'\"',
                },
            },
        ];

        for tc in test_cases {
            let bs = tc.csv.as_bytes();
            let got = DialectOptions::infer_from_sample(bs).unwrap();
            assert_eq!(tc.expected, got);
        }
    }

    #[test]
    fn typed_decode_ok() {
        struct TestCase {
            csv: String,
            types: Vec<DataType>,
            expected: Vec<Array>,
        }

        let test_cases = [
            // Simple
            TestCase {
                csv: [
                    "a,1,5.0\n", //
                    "b,2,5.5\n", //
                    "c,3,6.0\n",
                ]
                .join(""),
                types: vec![DataType::Utf8, DataType::Int64, DataType::Float64],
                expected: vec![
                    Array::Utf8(Utf8Array::from_iter(["a", "b", "c"])),
                    Array::Int64(Int64Array::from_iter([1, 2, 3])),
                    Array::Float64(Float64Array::from_iter([5.0, 5.5, 6.0])),
                ],
            },
            // Numbers in source, string as type
            TestCase {
                csv: [
                    "a,11\n",  //
                    "b,222\n", //
                    "c,3333\n",
                ]
                .join(""),
                types: vec![DataType::Utf8, DataType::Utf8],
                expected: vec![
                    Array::Utf8(Utf8Array::from_iter(["a", "b", "c"])),
                    Array::Utf8(Utf8Array::from_iter(["11", "222", "3333"])),
                ],
            },
        ];

        for tc in test_cases {
            let bs = tc.csv.as_bytes();
            let decoder = Decoder::new(csv_core::Reader::new(), None);
            let mut typed = TypedDecoder::new(tc.types, decoder);

            typed.decode(bs).unwrap();
            let got = typed.flush_skip(0).unwrap();

            assert_eq!(tc.expected, got);
        }
    }

    #[test]
    fn typed_decode_skip_records() {
        struct TestCase {
            csv: String,
            skip: usize,
            types: Vec<DataType>,
            expected: Vec<Array>,
        }

        let test_cases = [
            // Simple
            TestCase {
                csv: [
                    "a,1,5.0\n", //
                    "b,2,5.5\n", //
                    "c,3,6.0\n",
                ]
                .join(""),
                skip: 1,
                types: vec![DataType::Utf8, DataType::Int64, DataType::Float64],
                expected: vec![
                    Array::Utf8(Utf8Array::from_iter(["b", "c"])),
                    Array::Int64(Int64Array::from_iter([2, 3])),
                    Array::Float64(Float64Array::from_iter([5.5, 6.0])),
                ],
            },
            // Header (different types than values in the record)
            TestCase {
                csv: [
                    "column1, column2\n", //
                    "a,11\n",             //
                    "b,222\n",            //
                    "c,3333\n",
                ]
                .join(""),
                skip: 1,
                types: vec![DataType::Utf8, DataType::Utf8],
                expected: vec![
                    Array::Utf8(Utf8Array::from_iter(["a", "b", "c"])),
                    Array::Utf8(Utf8Array::from_iter(["11", "222", "3333"])),
                ],
            },
        ];

        for tc in test_cases {
            let bs = tc.csv.as_bytes();
            let decoder = Decoder::new(csv_core::Reader::new(), None);
            let mut typed = TypedDecoder::new(tc.types, decoder);

            typed.decode(bs).unwrap();
            let got = typed.flush_skip(tc.skip).unwrap();

            assert_eq!(tc.expected, got);
        }
    }
}
