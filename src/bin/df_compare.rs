use arrow::array::RecordBatch;
use arrow::util::display::array_value_to_string;
use datafusion::prelude::SessionContext;
use rQuery::db::{DBResult, Database};
use std::fs;

#[tokio::main]
async fn main() {
    let ctx = SessionContext::new();
    let mut db = Database::default();
    create_tables(&mut db);
    df_create_tables(&ctx).await;
    for i in 1..=5 {
        println!("Running query {}", i);
        let mine = execute_query(&mut db, i).join("\n");
        let df = df_execute_query(&ctx, i).await.join("\n");
        if mine == df {
            println!("Output matches");
        } else {
            panic!("Failed on query {i}. Mine num_rows: {}, df num_rows: {}", mine.len(), df.len())
        }
    }
}

fn execute_query(db: &mut Database, query: i32) -> Vec<String> {
    let path = format!("{}/data/queries/{query}.sql", env!("CARGO_MANIFEST_DIR"));
    let sql = fs::read_to_string(path).unwrap();
    let result = db.execute_sql(&sql);
    assert_eq!(result.len(), 1);
    let DBResult::Batch(batches) = &result[0] else {
        panic!("Expected a batch");
    };

    let mut rows = vec![];
    for batch in batches {
        rows.extend(stitch_rows(batch));
    }
    rows
}

async fn df_execute_query(ctx: &SessionContext, query: i32) -> Vec<String> {
    let path = format!("{}/data/queries/{query}.sql", env!("CARGO_MANIFEST_DIR"));
    let sql = fs::read_to_string(path).unwrap();
    let df = ctx.sql(&sql).await.unwrap();
    let result = df.collect().await.unwrap();
    let mut rows = vec![];
    for record in result {
        rows.extend(stitch_rows(&record));
    }

    rows
}

fn stitch_rows(batch: &RecordBatch) -> Vec<String> {
    let rows = batch.num_rows();
    let mut res = vec![];
    for i in 0..rows {
        res.push(stitch_row(&batch, i));
    }
    res
}

fn stitch_row(batch: &RecordBatch, row_num: usize) -> String {
    let mut values: Vec<String> = vec![];
    for col in batch.columns() {
        values.push(array_value_to_string(col, row_num).unwrap());
    }
    values.join(",")
}

async fn df_create_tables(ctx: &SessionContext) {
    let txt = include_str!("../../data/load.sql");
    let it = txt.split(";");
    // Create tables
    for mut s in it {
        s = s.trim();
        if s.contains("COPY") || s.is_empty() {
            continue;
        }
        let create_table = s.replace("CREATE TABLE", "CREATE EXTERNAL TABLE");

        let mut tokens = s.split(" ");
        let table_name = tokens
            .nth(2)
            .unwrap()
            .trim()
            .strip_suffix('(')
            .unwrap()
            .trim();
        let sql = format!(
            "{create_table} STORED AS CSV LOCATION 'data/tables/{table_name}.csv' OPTIONS ('has_header' 'false', 'delimiter' ',');"
        );
        let _ = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    }
}

fn create_tables(db: &mut Database) {
    let load_query: &str = include_str!("../../data/load.sql");
    let _ = db.execute_sql(load_query);
}
