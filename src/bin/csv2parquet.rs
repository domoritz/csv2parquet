use parquet::errors::ParquetError;
use to_parquet::CsvOpts;

fn main() -> Result<(), ParquetError> {
    to_parquet::run::<CsvOpts>()
}
