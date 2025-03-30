use std::fs::File;
use std::io::Seek;
use arrow::csv::reader::Format;
use arrow::csv::ReaderBuilder;
use arrow::datatypes::SchemaRef;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

fn main() {
    const sql_query: &str = &"select * from \"customers-100.csv\" where \"first name\" == 'Fred';";

    let dialect = GenericDialect {};
    let ast = Parser::parse_sql(&dialect, sql_query).unwrap();

    dbg!(&ast[0]);

}

fn read_csv() {
    const CSV_PATH: &str = "data/customers-100.csv";
    let mut file = File::open(CSV_PATH).unwrap();

    let (schema, _) = Format::default().with_header(true).infer_schema(&file, Some(5)).unwrap();
    file.rewind().unwrap();

    let csv = ReaderBuilder::new(SchemaRef::from(schema)).with_header(true).build(file).unwrap();
    for batch in csv {
        let batch = batch.unwrap();
        dbg!(batch);
    }
}
