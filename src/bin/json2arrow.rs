use arrow::error::ArrowError;
use to_parquet::{ArrowOpts, JsonOpts};

fn main() -> Result<(), ArrowError> {
    to_parquet::run::<JsonOpts, ArrowOpts>()
}
