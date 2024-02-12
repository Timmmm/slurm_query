use arrow::{
    array::ArrayRef,
    util::display::{ArrayFormatter, FormatOptions},
};

pub fn value_string(column: &ArrayRef, row: usize) -> String {
    let options = FormatOptions::default();
    match ArrayFormatter::try_new(column.as_ref(), &options) {
        Ok(f) => f.value(row).to_string(),
        Err(e) => e.to_string(),
    }
}
