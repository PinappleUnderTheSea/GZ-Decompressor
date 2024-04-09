use std::fs::File;
use std::sync::Arc;
use csv::ReaderBuilder;
use flate2::read::GzDecoder;
use parquet::file::writer::SerializedFileWriter;
use parquet::record::RecordWriter;
use parquet::schema::parser::parse_message_type;
use parquet_derive::ParquetRecordWriter;


#[derive(ParquetRecordWriter)]
struct ACompleteRecord {
    pub a_int:  i32,
    pub b_int:  i32,
    pub a_string: String,
}

const BATCH_SIZE: usize = 1000;
const INPUT_FILE: &str ="test.csv.gz";
const OUTPUT_FILE: &str = "large_file.parquet";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 打开 gzip 文件
    let gz = File::open(INPUT_FILE)?;
    let decoder = GzDecoder::new(gz);

    // 创建 csv reader
    let mut rdr = ReaderBuilder::new().has_headers(false).from_reader(decoder);

    // 创建 parquet writer
    let file = File::create(OUTPUT_FILE)?;
    let schema = Arc::new(parse_message_type(
        "
    message schema {
        REQUIRED INT32 a;
        REQUIRED INT32 b;
        REQUIRED BYTE_ARRAY c_string (UTF8);
    }
    "
    )?);

    let mut writer = SerializedFileWriter::new(file, schema, Default::default())?;
    let mut rows = Vec::with_capacity(BATCH_SIZE);
    for result in rdr.records() {
        let record = result?;
        let row = ACompleteRecord {
            a_int: record.get(0).unwrap().parse::<i32>().unwrap(),
            b_int: record.get(1).unwrap().parse::<i32>().unwrap(),
            a_string:record.get(2).unwrap().to_string(),
        };
        rows.push(row);
        // println!("aaa");
        if rows.len() == BATCH_SIZE {
            let mut row_group_writer = writer.next_row_group()?;
            rows.as_slice().write_to_row_group(&mut row_group_writer).unwrap();
            row_group_writer.close()?;
            rows.clear();
        }
    }
    if rows.len() != BATCH_SIZE {
        let mut row_group_writer = writer.next_row_group()?;
        // println!("len = {} {} {} {}", rows.len(), rows[1].a_int, rows[1].b_int, rows[1].a_string);
        rows.as_slice().write_to_row_group(&mut row_group_writer).unwrap();
        row_group_writer.close()?;
        rows.clear();
    }
    writer.close()?;
    Ok(())
}