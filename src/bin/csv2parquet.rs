use arrow::error::ArrowError;
use to_parquet::{CsvOpts, ParquetOpts};

fn main() -> Result<(), ArrowError> {
    to_parquet::run::<CsvOpts, ParquetOpts>()
}
