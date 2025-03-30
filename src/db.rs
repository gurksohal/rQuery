use crate::executor::build_executor_plan;
use crate::logical_plan::LogicalPlan;
use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, SchemaBuilder, SchemaRef};
use sqlparser::ast;
use sqlparser::ast::{
    ColumnOption, CopyOption, CopySource, CopyTarget, CreateTable, Query, Statement,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

const DIALECT: GenericDialect = GenericDialect {};

#[derive(Debug)]
pub enum DBResult {
    String(String),
    Batch(Vec<RecordBatch>),
}

pub(crate) struct TableMetadata {
    pub(crate) schema: SchemaRef,
    pub(crate) file_location: Option<PathBuf>,
    pub(crate) delimiter: u8,
    pub(crate) header: bool,
}

impl TableMetadata {
    fn new_with_schema(schema: SchemaRef) -> Self {
        TableMetadata {
            schema,
            file_location: None,
            delimiter: b',',
            header: true,
        }
    }
}

#[derive(Default)]
pub struct Database {
    catalog: HashMap<String, TableMetadata>,
}

impl Database {
    pub fn execute_sql(&mut self, sql_cmd: &str) -> Vec<DBResult> {
        let ast = Parser::parse_sql(&DIALECT, sql_cmd)
            .unwrap_or_else(|e| panic!("Failed to parse SQL: {e}"));

        let mut results = vec![];
        for statement in ast {
            let res = match statement {
                Statement::Query(query) => self.execute_query(*query),
                copy_stmt @ Statement::Copy { .. } => self.copy_table(copy_stmt),
                Statement::CreateTable(create_stmt) => self.create_table(create_stmt),
                _other => {
                    panic!("ERROR: Unsupported statement: {_other:?}");
                }
            };

            results.push(res);
        }

        results
    }

    fn execute_query(&self, query_stmt: Query) -> DBResult {
        let mut p = LogicalPlan::build_logical_plan(query_stmt);
        p = LogicalPlan::rewrite(p, &self.catalog);

        let mut physical_plan = build_executor_plan(p, &self.catalog);
        let mut res = vec![];
        while let Some(Ok(p)) = physical_plan.next() {
            res.push(p);
        }

        DBResult::Batch(res)
    }

    fn create_table(&mut self, create_stmt: CreateTable) -> DBResult {
        assert_eq!(
            create_stmt.name.0.len(),
            1,
            "expected 1 table name. got: {:?}",
            create_stmt.name.0
        );

        let table_name = &create_stmt.name.0[0]
            .as_ident()
            .expect("Unable to get as ident (got none)")
            .value;

        let mut schema_builder = SchemaBuilder::new();
        for col in create_stmt.columns {
            let name = col.name.value;
            let data_type = Self::get_arrow_datatype(col.data_type);
            let mut nullable = true;
            if !col.options.is_empty() {
                assert_eq!(
                    col.options.len(),
                    1,
                    "Expected only 1 option, got: {:?}",
                    col.options
                );
                nullable = !matches!(col.options[0].option, ColumnOption::NotNull);
            }

            let field = Field::new(name, data_type, nullable);
            schema_builder.push(field);
        }

        assert!(!self.catalog.contains_key(table_name));
        self.catalog.insert(
            table_name.to_string(),
            TableMetadata::new_with_schema(schema_builder.finish().into()),
        );

        DBResult::String(format!("Created {} table", table_name))
    }

    fn copy_table(&mut self, stmt: Statement) -> DBResult {
        if let Statement::Copy {
            source,
            to: _to,
            target,
            options,
            legacy_options: _legacy_options,
            values: _values,
        } = &stmt
        {
            let CopySource::Table { table_name, .. } = source else {
                panic!("COPY only supports table as dst, got: {stmt:?}");
            };
            assert_eq!(table_name.0.len(), 1, "Only 1 table source supported");
            let name = &table_name.0.first().unwrap().as_ident().unwrap().value;
            let CopyTarget::File { filename } = target else {
                panic!("COPY only supports file as src")
            };

            assert!(
                fs::exists(filename).unwrap_or(false),
                "Source file must exist"
            );
            assert!(
                self.catalog.contains_key(name),
                "Table must be created before copying. Trying to add {}, current catalog: {:?}",
                name,
                self.catalog.keys()
            );

            for opt in options {
                match opt {
                    CopyOption::Delimiter(c) => {
                        self.catalog.get_mut(name).unwrap().delimiter =
                            u8::try_from(c.to_owned()).unwrap()
                    }
                    CopyOption::Header(header) => {
                        self.catalog.get_mut(name).unwrap().header = header.to_owned()
                    }
                    _other => panic!("COPY does not support {_other:?} as an option"),
                }
            }

            self.catalog.get_mut(name).unwrap().file_location = Some(filename.into());
            return DBResult::String(format!("Copied {name} from {filename}"));
        };

        unreachable!()
    }

    fn get_arrow_datatype(datatype: ast::DataType) -> DataType {
        match datatype {
            ast::DataType::Char(_) | ast::DataType::Varchar(_) => DataType::Utf8,
            ast::DataType::BigInt(_) | ast::DataType::Integer(_) => DataType::Int64,
            ast::DataType::Boolean => DataType::Boolean,
            _ => panic!("ERROR: {datatype:?} not supported"),
        }
    }
}
