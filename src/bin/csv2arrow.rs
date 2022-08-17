use arrow::error::ArrowError;
use to_parquet::{ArrowOpts, CsvOpts};

fn main() -> Result<(), ArrowError> {
    to_parquet::run::<CsvOpts, ArrowOpts>()
}
