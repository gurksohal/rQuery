use std::{fs, io};
use std::io::Write;
use arrow::array::RecordBatch;
use arrow::util::display::array_value_to_string;
use rQuery::db::{DBResult, Database};

fn main() {
    const LOAD_QUERY: &str = include_str!("../data/load.sql");
    let mut db = Database::default();

    loop {
        let mut input = String::new();
        print!("> ");
        io::stdout().flush().unwrap();
        if io::stdin().read_line(&mut input).is_err() {
            println!("Error reading input");
            continue;
        }

        let input = input.trim();
        if input == "exit" {
            break;
        }

        if input == "load" {
            for s in db.execute_sql(LOAD_QUERY) {
                if let DBResult::String(s) = s{
                    println!("{}", s);
                }
            }
        } else if input.starts_with("q ") {
            let tokens = input.split_whitespace().collect::<Vec<&str>>();
            if tokens.len() != 2 {
                println!("Invalid command. q 1 through q 5 is valid");
                continue;
            }
            let query_num = tokens[1];
            let path = format!("{}/data/queries/{query_num}.sql", env!("CARGO_MANIFEST_DIR"));
            let sql = fs::read_to_string(path).unwrap();
            println!("Executing: {}\n", sql);
            execute_sql(&mut db, &sql);
        } else {
            execute_sql(&mut db, input);
        }
    }
}

fn execute_sql(db: &mut Database, sql: &str) {
    let mut sql_res = db.execute_sql(sql);
    if sql_res.len() != 1 {
        println!("Only single sql statement accepted");
        return;
    }
    let sql_res = sql_res.pop().unwrap();
    match sql_res {
        DBResult::String(s) => println!("{}", s),
        DBResult::Batch(batches) => {
            let mut res = vec![];
            for batch in batches {
                res.extend(record_batch_to_row(&batch));
            }
            println!("{}", res.join("\n"));
        }
    }
}

fn record_batch_to_row(batch: &RecordBatch) -> Vec<String> {
    let mut rows = vec![];
    for i in 0..batch.num_rows() {
        let mut values: Vec<String> = vec![];
        for col in batch.columns() {
            values.push(array_value_to_string(col, i).unwrap());
        }
        rows.push(values.join(","));
    }
    rows
}
