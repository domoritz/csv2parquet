use parquet::errors::ParquetError;
use to_parquet::JsonOpts;

fn main() -> Result<(), ParquetError> {
    to_parquet::run::<JsonOpts>()
}
