use crate::{
    batch::Batch,
    field::Schema,
    format::{FormatOptions, Formatter},
};
use rayexec_error::Result;
use std::fmt::Write as _;

pub fn ugly_print<'a, I>(schema: &Schema, batches: I) -> Result<String>
where
    I: IntoIterator<Item = &'a Batch>,
{
    const OPTS: FormatOptions = FormatOptions::new();
    let formatter = Formatter::new(OPTS);

    let mut buf = schema
        .iter()
        .map(|f| f.name.clone())
        .collect::<Vec<_>>()
        .join("\t");
    write!(buf, "\n")?;

    for batch in batches.into_iter() {
        for idx in 0..batch.num_rows() {
            for col in batch.columns() {
                write!(
                    buf,
                    "{}\t",
                    formatter
                        .format_array_value(col, idx)
                        .expect("value to exist")
                )?;
            }
            write!(buf, "\n")?;
        }
    }

    Ok(buf)
}
