use arrow::error::ArrowError;
use to_parquet::{JsonOpts, ParquetOpts};

fn main() -> Result<(), ArrowError> {
    to_parquet::run::<JsonOpts, ParquetOpts>()
}
