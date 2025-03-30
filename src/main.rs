use rQuery::db::Database;

fn main() {
    const LOAD_QUERY: &str = include_str!("../data/load.sql");
    const SQL_QUERY: &str = include_str!("../data/queries/1.sql");

    let mut db = Database::default();
    let _ = db.execute_sql(LOAD_QUERY);
    let _ = db.execute_sql(SQL_QUERY);
}
