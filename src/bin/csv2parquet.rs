use to_parquet::CsvOpts;
use parquet::errors::ParquetError;

fn main() -> Result<(), ParquetError> {
    to_parquet::run::<CsvOpts>()
}
