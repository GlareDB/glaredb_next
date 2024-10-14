use rayexec_error::{OptionExt, RayexecError, Result};

use crate::thrift_gen;
use crate::types::{
    ConvertedType,
    GroupType,
    LogicalType,
    ParquetType,
    PhysicalType,
    PrimitiveType,
    Repetition,
    TypeInfo,
};

/// Physical type for leaf-level primitive columns.
///
/// Also includes the maximum definition and repetition levels required to
/// re-assemble nested data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDescriptor {
    /// The "leaf" primitive type of this column
    pub primitive_type: PrimitiveType,
    /// The maximum definition level for this column
    pub max_def_level: i16,
    /// The maximum repetition level for this column
    pub max_rep_level: i16,
    /// Path with all parts to the column.
    pub path: ColumnPath,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnPath {
    pub parts: Vec<String>,
}

/// Schema of a parquet file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    /// All leaves in the schema.
    pub leaves: Vec<ColumnDescriptor>,
    /// Root indices for each leaf pointing back to the top-level root which
    /// holds the leaf.
    pub leaves_to_roots: Vec<usize>,
}

impl Schema {
    pub fn try_from_thrift(elements: &[thrift_gen::SchemaElement]) -> Result<Self> {
        if elements.is_empty() {
            return Err(RayexecError::new("Missing schema root"));
        }

        // let mut out = Vec::with_capacity(elements.len());
        // let mut leaves = Vec::new();

        // walk_elements(0, elements, &mut out, &mut leaves)?;

        // Ok(Schema {
        //     leaves,
        //     elements: out,
        // })
        unimplemented!()
    }
}

fn walk_elements<'a>(
    root_idx: usize,
    mut element_idx: usize,
    mut max_rep_level: i16,
    mut max_def_level: i16,
    elements: &'a [thrift_gen::SchemaElement],
    leaves: &mut Vec<ColumnDescriptor>,
    leaves_to_roots: &mut Vec<usize>,
    curr_path: &mut Vec<&'a str>,
) -> Result<usize> {
    while element_idx < elements.len() {
        let element = &elements[element_idx];

        curr_path.push(&element.name);

        // Every field other than root requires repetition.
        let repetition: Repetition = element
            .repetition_type
            .required("field repetition")?
            .try_into()?;

        match repetition {
            Repetition::Optional => {
                max_def_level += 1;
            }
            Repetition::Repeated => {
                max_def_level += 1;
                max_rep_level += 1;
            }
            _ => (),
        }

        // Optional converted type.
        let converted_type = element
            .converted_type
            .map(ConvertedType::try_from)
            .transpose()?;

        // Optional logical type
        let logical_type = element.logical_type.clone().map(LogicalType::from);

        match element.num_children {
            None | Some(0) => {
                // Leaf node.

                let physical_type: PhysicalType =
                    element.type_.required("physical type")?.try_into()?;

                let typ = PrimitiveType {
                    info: TypeInfo {
                        name: element.name.clone(),
                        repetition,
                        converted_type,
                        logical_type,
                        id: element.field_id,
                    },
                    physical_type,
                    type_length: element.type_length.unwrap_or_default(),
                };

                let path: Vec<_> = curr_path.iter().copied().map(|s| s.to_string()).collect();
                leaves.push(ColumnDescriptor {
                    primitive_type: typ,
                    max_def_level,
                    max_rep_level,
                    path: ColumnPath { parts: path },
                });
                leaves_to_roots.push(root_idx);

                element_idx += 1;
            }
            Some(n) => {
                // let mut field_indices = Vec::with_capacity(n as usize);

                let mut next = element_idx + 1;

                for _ in 0..n {
                    next = walk_elements(
                        root_idx,
                        next,
                        max_rep_level,
                        max_def_level,
                        elements,
                        leaves,
                        leaves_to_roots,
                        curr_path,
                    )?;
                    curr_path.pop();
                }

                element_idx = next;
            }
        }
    }

    Ok(element_idx)
}
