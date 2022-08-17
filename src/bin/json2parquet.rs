use to_parquet::JsonOpts;
use parquet::errors::ParquetError;

fn main() -> Result<(), ParquetError> {
    to_parquet::run::<JsonOpts>()
}
