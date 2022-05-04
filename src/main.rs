use arrow::csv::ReaderBuilder;
use clap::{Parser, ValueHint};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, Encoding},
    errors::ParquetError,
    file::properties::WriterProperties,
};
use serde_json::{to_string_pretty, Value};
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(clap::ArgEnum, Clone)]
#[allow(non_camel_case_types, clippy::upper_case_acronyms)]
enum ParquetCompression {
    UNCOMPRESSED,
    SNAPPY,
    GZIP,
    LZO,
    BROTLI,
    LZ4,
    ZSTD,
}

#[derive(clap::ArgEnum, Clone)]
#[allow(non_camel_case_types, clippy::upper_case_acronyms)]
enum ParquetEncoding {
    PLAIN,
    RLE,
    BIT_PACKED,
    DELTA_BINARY_PACKED,
    DELTA_LENGTH_BYTE_ARRAY,
    DELTA_BYTE_ARRAY,
    RLE_DICTIONARY,
}

#[derive(Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"), author = "Dominik Moritz <domoritz@cmu.edu>")]
struct Opts {
    /// Input CSV file.
    #[clap(name = "CSV", parse(from_os_str), value_hint = ValueHint::AnyPath)]
    input: PathBuf,

    /// Output file.
    #[clap(name = "PARQUET", parse(from_os_str), value_hint = ValueHint::AnyPath)]
    output: PathBuf,

    /// Arrow schema to be applied to data in CSV (same format as written out by -p / -n). {n}
    /// {n}
    /// If this value is supplied, no attempt will be made to infer a schema from the data. {n}
    /// {n}
    /// This feature may be useful to you if the inferred schema does not suit your needs (for example
    /// the inference process favours defining large types for the schema where sometimes a smaller
    /// type would suffice (i.e. using Int64 rather than a Uint16). {n}
    /// {n}
    /// For more detail about exactly how a schema may be structured, take a look at the source of
    /// DataType fn from(json: &Value -> Result<DataType> in: {n}
    /// {n}
    /// https://github.com/apache/arrow-rs/blob/master/arrow/src/datatypes/datatype.rs {n}
    /// {n}
    /// Make sure to have the same number of columns in your schema file as are in your CSV!
    #[clap(short = 's', long, parse(from_os_str), value_hint = ValueHint::AnyPath)]
    schema_def_file: Option<PathBuf>,

    /// The number of records to infer the schema from. All rows if not present. Setting max-read-records to zero will stop schema inference and all columns will be string typed.
    #[clap(long)]
    max_read_records: Option<usize>,

    /// Set whether the CSV file has headers
    #[clap(short, long)]
    header: Option<bool>,

    /// Set the CSV file's column delimiter as a byte character.
    #[clap(short, long, default_value = ",")]
    delimiter: char,

    /// Set the compression.
    #[clap(short, long, arg_enum)]
    compression: Option<ParquetCompression>,

    /// Sets encoding for any column.
    #[clap(short, long, arg_enum)]
    encoding: Option<ParquetEncoding>,

    /// Sets data page size limit.
    #[clap(long)]
    data_pagesize_limit: Option<usize>,

    /// Sets dictionary page size limit.
    #[clap(long)]
    dictionary_pagesize_limit: Option<usize>,

    /// Sets write batch size.
    #[clap(long)]
    write_batch_size: Option<usize>,

    /// Sets max size for a row group.
    #[clap(long)]
    max_row_group_size: Option<usize>,

    /// Sets "created by" property.
    #[clap(long)]
    created_by: Option<String>,

    /// Sets flag to enable/disable dictionary encoding for any column.
    #[clap(long)]
    dictionary: bool,

    /// Sets flag to enable/disable statistics for any column.
    #[clap(long)]
    statistics: bool,

    /// Sets max statistics size for any column. Applicable only if statistics are enabled.
    #[clap(long)]
    max_statistics_size: Option<usize>,

    /// Print the schema to stderr.
    #[clap(short, long)]
    print_schema: bool,

    /// Only print the schema
    #[clap(short = 'n', long)]
    dry: bool,
}

fn main() -> Result<(), ParquetError> {
    let opts: Opts = Opts::parse();

    let mut input = File::open(opts.input)?;

    let schema = match opts.schema_def_file {
        Some(schema_def_file_path) => {
            // a schema definition file path has been provided, create schema from that
            let schema_file = match File::open(&schema_def_file_path) {
                Ok(file) => Ok(file),
                Err(open_schema_file_err) => Err(ParquetError::General(format!(
                    "Error opening schema file: {:?}, message: {}",
                    schema_def_file_path, open_schema_file_err
                ))),
            }?;
            let json_read_result: serde_json::Result<Value> = serde_json::from_reader(schema_file);
            match json_read_result {
                Ok(schema_json) => match arrow::datatypes::Schema::from(&schema_json) {
                    Ok(schema) => Ok(schema),
                    Err(schema_err) => Err(ParquetError::ArrowError(schema_err.to_string())),
                },
                Err(err) => Err(ParquetError::General(format!(
                    "Error reading schema json: {}",
                    err
                ))),
            }
        }
        _ => {
            // infer schema from file contents
            // NOTE: if max_read_records is zero then all cols are assumed to be "string"
            match arrow::csv::reader::infer_file_schema(
                &mut input,
                opts.delimiter as u8,
                opts.max_read_records,
                opts.header.unwrap_or(true),
            ) {
                Ok((schema, _inferred_has_header)) => Ok(schema),
                Err(infer_schema_err) => Err(ParquetError::General(format!(
                    "Error inferring schema: {}",
                    infer_schema_err
                ))),
            }
        }
    }?;

    if opts.print_schema || opts.dry {
        let json: String = to_string_pretty(&schema.to_json()).unwrap();
        eprintln!("Schema:");
        println!("{}", json);
        if opts.dry {
            return Ok(());
        }
    }

    //let reader = builder.build(input)?;
    let schema_ref = Arc::new(schema);
    let builder = ReaderBuilder::new()
        .has_header(opts.header.unwrap_or(true))
        .with_delimiter(opts.delimiter as u8)
        .with_schema(schema_ref);

    let reader = builder.build(input)?;

    let output = File::create(opts.output)?;

    let mut props = WriterProperties::builder()
        .set_dictionary_enabled(opts.dictionary)
        .set_statistics_enabled(opts.statistics);

    if let Some(compression) = opts.compression {
        let compression = match compression {
            ParquetCompression::UNCOMPRESSED => Compression::UNCOMPRESSED,
            ParquetCompression::SNAPPY => Compression::SNAPPY,
            ParquetCompression::GZIP => Compression::GZIP,
            ParquetCompression::LZO => Compression::LZO,
            ParquetCompression::BROTLI => Compression::BROTLI,
            ParquetCompression::LZ4 => Compression::LZ4,
            ParquetCompression::ZSTD => Compression::ZSTD,
        };

        props = props.set_compression(compression);
    }

    if let Some(encoding) = opts.encoding {
        let encoding = match encoding {
            ParquetEncoding::PLAIN => Encoding::PLAIN,
            ParquetEncoding::RLE => Encoding::RLE,
            ParquetEncoding::BIT_PACKED => Encoding::BIT_PACKED,
            ParquetEncoding::DELTA_BINARY_PACKED => Encoding::DELTA_BINARY_PACKED,
            ParquetEncoding::DELTA_LENGTH_BYTE_ARRAY => Encoding::DELTA_LENGTH_BYTE_ARRAY,
            ParquetEncoding::DELTA_BYTE_ARRAY => Encoding::DELTA_BYTE_ARRAY,
            ParquetEncoding::RLE_DICTIONARY => Encoding::RLE_DICTIONARY,
        };

        props = props.set_encoding(encoding);
    }

    if let Some(size) = opts.write_batch_size {
        props = props.set_write_batch_size(size);
    }

    if let Some(size) = opts.data_pagesize_limit {
        props = props.set_data_pagesize_limit(size);
    }

    if let Some(size) = opts.dictionary_pagesize_limit {
        props = props.set_dictionary_pagesize_limit(size);
    }

    if let Some(size) = opts.dictionary_pagesize_limit {
        props = props.set_dictionary_pagesize_limit(size);
    }

    if let Some(size) = opts.max_row_group_size {
        props = props.set_max_row_group_size(size);
    }

    if let Some(created_by) = opts.created_by {
        props = props.set_created_by(created_by);
    }

    if let Some(size) = opts.max_statistics_size {
        props = props.set_max_statistics_size(size);
    }

    let mut writer = ArrowWriter::try_new(output, reader.schema(), Some(props.build()))?;

    for batch in reader {
        match batch {
            Ok(batch) => writer.write(&batch)?,
            Err(error) => return Err(error.into()),
        }
    }

    match writer.close() {
        Ok(_) => Ok(()),
        Err(error) => Err(error),
    }
}
