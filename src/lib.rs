use arrow::csv;
use arrow::datatypes::Schema;
use arrow::error::ArrowError;
use arrow::json;
use arrow::record_batch::RecordBatch;
use clap::{Parser, ValueHint};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, Encoding},
    file::properties::{EnabledStatistics, WriterProperties},
};
use serde_json::{to_string_pretty, Value};
use std::fs::File;
use std::io::BufReader;
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

#[derive(clap::ArgEnum, Clone)]
#[allow(non_camel_case_types, clippy::upper_case_acronyms)]
enum ParquetEnabledStatistics {
    None,
    Chunk,
    Page,
}

#[derive(Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"), author = "Dominik Moritz <domoritz@cmu.edu>")]
struct Opts<I: clap::Args, O: clap::Args> {
    /// Input file.
    #[clap(name = "INPUT", parse(from_os_str), value_hint = ValueHint::AnyPath)]
    input: PathBuf,

    /// Output file.
    #[clap(name = "OUTPUT", parse(from_os_str), value_hint = ValueHint::AnyPath)]
    output: PathBuf,

    /// File with Arrow schema in JSON format.
    #[clap(short = 's', long, parse(from_os_str), value_hint = ValueHint::AnyPath)]
    schema_file: Option<PathBuf>,

    /// The number of records to infer the schema from. All rows if not present. Setting max-read-records to zero will stop schema inference and all columns will be string typed.
    #[clap(long)]
    max_read_records: Option<usize>,

    /// Options specific to the input format we're parsing
    #[clap(flatten)]
    input_format: I,

    /// Options specific to the output format we're writing
    #[clap(flatten)]
    output_format: O,

    /// Print the schema to stderr.
    #[clap(short, long)]
    print_schema: bool,

    /// Only print the schema
    #[clap(short = 'n', long)]
    dry: bool,
}

#[derive(clap::Args)]
pub struct CsvOpts {
    /// Set whether the CSV file has headers
    #[clap(short, long)]
    header: Option<bool>,

    /// Set the CSV file's column delimiter as a byte character.
    #[clap(short, long, default_value = ",")]
    delimiter: char,
}

#[derive(clap::Args)]
pub struct JsonOpts;

#[derive(clap::Args)]
pub struct ParquetOpts {
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
    #[clap(long, arg_enum)]
    statistics: Option<ParquetEnabledStatistics>,

    /// Sets max statistics size for any column. Applicable only if statistics are enabled.
    #[clap(long)]
    max_statistics_size: Option<usize>,
}

pub fn run<I, O>() -> Result<(), ArrowError>
where
    I: clap::Args,
    I: InputFormat,
    <I as InputFormat>::Reader: Iterator<Item = Result<RecordBatch, ArrowError>>,
    O: clap::Args,
    O: OutputFormat,
{
    let opts: Opts<I, O> = Opts::parse();

    let mut input = File::open(&opts.input.as_path())?;

    let schema = match &opts.schema_file {
        Some(schema_def_file_path) => {
            let schema_file = match File::open(&schema_def_file_path.as_path()) {
                Ok(file) => Ok(file),
                Err(error) => Err(ArrowError::IoError(format!(
                    "Error opening schema file: {:?}, message: {}",
                    schema_def_file_path, error
                ))),
            }?;
            let json: serde_json::Result<Value> = serde_json::from_reader(schema_file);
            match json {
                Ok(schema_json) => match arrow::datatypes::Schema::from(&schema_json) {
                    Ok(schema) => Ok(schema),
                    Err(error) => Err(error),
                },
                Err(err) => Err(ArrowError::IoError(format!(
                    "Error reading schema json: {}",
                    err
                ))),
            }
        }
        _ => {
            match opts
                .input_format
                .infer_file_schema(opts.max_read_records, &mut input)
            {
                Ok(schema) => Ok(schema),
                Err(error) => Err(ArrowError::SchemaError(format!(
                    "Error inferring schema: {}",
                    error
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

    let schema_ref = Arc::new(schema);

    let reader = opts.input_format.make_reader(schema_ref.clone(), input)?;

    let output = File::create(opts.output)?;

    let mut writer = opts.output_format.try_new_writer(output, schema_ref)?;

    for batch in reader {
        match batch {
            Ok(batch) => opts.output_format.write(&mut writer, &batch)?,
            Err(error) => return Err(error),
        }
    }

    match opts.output_format.close(writer) {
        Ok(_) => Ok(()),
        Err(error) => Err(error),
    }
}

pub trait InputFormat {
    type Reader;

    fn infer_file_schema(
        &self,
        max_read_records: Option<usize>,
        input: &mut File,
    ) -> arrow::error::Result<Schema>;

    fn make_reader(
        &self,
        schema_ref: Arc<Schema>,
        input: File,
    ) -> arrow::error::Result<Self::Reader>;
}

impl InputFormat for CsvOpts {
    type Reader = csv::Reader<File>;

    fn infer_file_schema(
        &self,
        max_read_records: Option<usize>,
        input: &mut File,
    ) -> arrow::error::Result<Schema> {
        csv::reader::infer_file_schema(
            input,
            self.delimiter as u8,
            max_read_records,
            self.header.unwrap_or(true),
        )
        .map(|(s, _)| s)
    }

    fn make_reader(
        &self,
        schema_ref: Arc<Schema>,
        input: File,
    ) -> arrow::error::Result<csv::Reader<File>> {
        let builder = csv::ReaderBuilder::new()
            .has_header(self.header.unwrap_or(true))
            .with_delimiter(self.delimiter as u8)
            .with_schema(schema_ref);
        builder.build(input)
    }
}

impl InputFormat for JsonOpts {
    type Reader = json::Reader<File>;

    fn infer_file_schema(
        &self,
        max_read_records: Option<usize>,
        input: &mut File,
    ) -> arrow::error::Result<Schema> {
        let mut buf = BufReader::new(input);
        json::reader::infer_json_schema_from_seekable(&mut buf, max_read_records)
    }

    fn make_reader(
        &self,
        schema_ref: Arc<Schema>,
        input: File,
    ) -> arrow::error::Result<Self::Reader> {
        let builder = json::ReaderBuilder::new().with_schema(schema_ref);
        builder.build(input)
    }
}

pub trait OutputFormat {
    type Writer;

    fn try_new_writer(
        &self,
        output: File,
        schema_ref: Arc<Schema>,
    ) -> arrow::error::Result<Self::Writer>;

    fn write(&self, writer: &mut Self::Writer, batch: &RecordBatch) -> arrow::error::Result<()>;

    fn close(&self, writer: Self::Writer) -> arrow::error::Result<()>;
}

impl OutputFormat for ParquetOpts {
    type Writer = ArrowWriter<File>;

    fn try_new_writer(
        &self,
        output: File,
        schema_ref: Arc<Schema>,
    ) -> arrow::error::Result<Self::Writer> {
        let mut props = WriterProperties::builder().set_dictionary_enabled(self.dictionary);

        if let Some(statistics) = &self.statistics {
            let statistics = match statistics {
                ParquetEnabledStatistics::Chunk => EnabledStatistics::Chunk,
                ParquetEnabledStatistics::Page => EnabledStatistics::Page,
                ParquetEnabledStatistics::None => EnabledStatistics::None,
            };

            props = props.set_statistics_enabled(statistics);
        }

        if let Some(compression) = &self.compression {
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

        if let Some(encoding) = &self.encoding {
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

        if let Some(size) = self.write_batch_size {
            props = props.set_write_batch_size(size);
        }

        if let Some(size) = self.data_pagesize_limit {
            props = props.set_data_pagesize_limit(size);
        }

        if let Some(size) = self.dictionary_pagesize_limit {
            props = props.set_dictionary_pagesize_limit(size);
        }

        if let Some(size) = self.dictionary_pagesize_limit {
            props = props.set_dictionary_pagesize_limit(size);
        }

        if let Some(size) = self.max_row_group_size {
            props = props.set_max_row_group_size(size);
        }

        if let Some(created_by) = &self.created_by {
            props = props.set_created_by(created_by.clone());
        }

        if let Some(size) = self.max_statistics_size {
            props = props.set_max_statistics_size(size);
        }

        ArrowWriter::try_new(output, schema_ref, Some(props.build())).map_err(|err| err.into())
    }

    fn write(&self, writer: &mut Self::Writer, batch: &RecordBatch) -> arrow::error::Result<()> {
        writer.write(batch).map_err(|err| err.into())
    }

    fn close(&self, writer: Self::Writer) -> arrow::error::Result<()> {
        writer.close().map(|_| ()).map_err(|err| err.into())
    }
}
