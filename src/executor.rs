use crate::db::TableMetadata;
use crate::logical_plan::{Expr, LogicalPlan, Operator, ScalarValue, SortExpr};
use arrow::array::{
    Array, ArrayRef, AsArray, BooleanBuilder, Int64Builder, RecordBatch, StringBuilder,
    UInt32Builder,
};
use arrow::compute::kernels::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use arrow::compute::kernels::numeric::{add, div, mul, sub};
use arrow::compute::{
    SortColumn, SortOptions, and_kleene, concat_batches, filter_record_batch, lexsort_to_indices,
    or_kleene, sort_to_indices, take,
};
use arrow::csv;
use arrow::csv::ReaderBuilder;
use arrow::datatypes::{Field, Schema};
use arrow::error::ArrowError;
use arrow::error::Result as ArrowResult;
use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;

pub fn build_executor_plan(
    plan: Arc<LogicalPlan>,
    catalog: &HashMap<String, TableMetadata>,
) -> Box<dyn Executor> {
    match plan.as_ref() {
        LogicalPlan::Scan(s) => {
            let meta = catalog.get(&s.table_name).unwrap_or_else(|| {
                panic!(
                    "Trying to get {}, current tables: {:?}",
                    &s.table_name,
                    catalog.keys()
                )
            });
            let reader = ReaderBuilder::new(meta.schema.clone())
                .with_header(meta.header)
                .with_delimiter(meta.delimiter)
                .build(File::open(meta.file_location.clone().unwrap()).unwrap())
                .unwrap();
            Box::new(ScanExec {
                reader,
                table_name: s.table_name.clone(),
            })
        }
        LogicalPlan::Projection(p) => {
            let child = build_executor_plan(p.input.clone(), catalog);
            let p = ProjectExec {
                expressions: p.expr.clone(),
                child,
            };
            Box::new(p)
        }
        LogicalPlan::Filter(f) => {
            let child = build_executor_plan(f.input.clone(), catalog);
            let f = FilterExec {
                predicate: f.predicate.clone(),
                child,
            };
            Box::new(f)
        }
        LogicalPlan::Aggregate(_a) => {
            // let child = build_executor_plan(a.input.clone(), catalog);
            // TODO: Create AggregateExec
            todo!()
        }
        LogicalPlan::Sort(s) => {
            let child = build_executor_plan(s.input.clone(), catalog);
            let s = SortExec {
                exprs: s.expr.clone(),
                finished: false,
                child,
            };
            Box::new(s)
        }
        LogicalPlan::Join(j) => {
            let left = build_executor_plan(j.left.clone(), catalog);
            let right = build_executor_plan(j.right.clone(), catalog);
            assert_eq!(j.on.len(), 1, "empty joins not supported");
            let j = JoinExec {
                finished: false,
                left,
                right,
                predicate: j.on.first().unwrap().clone(),
            };
            Box::new(j)
        }
    }
}

fn get_column_name(name: &str, table_name: &Option<String>) -> String {
    if table_name.is_none() {
        return name.to_string();
    }
    let mut ret = table_name.clone().unwrap();
    ret.push('.');
    ret.push_str(&name);
    ret
}

fn evaluate_expr(expr: &Expr, batch: &RecordBatch) -> ArrowResult<ArrayRef> {
    match expr {
        Expr::Alias(a) => evaluate_expr(a.expr.as_ref(), batch),
        Expr::Column(c) => {
            let idx = batch
                .schema()
                .index_of(get_column_name(&c.name, &c.table_name).as_str())?;
            Ok(batch.column(idx).clone())
        }
        Expr::Literal(val) => {
            let len = batch.num_rows();
            match val {
                ScalarValue::Boolean(b) => {
                    let mut builder = BooleanBuilder::new();
                    let values = (0..len).map(|_| Some(*b)).collect::<Vec<Option<bool>>>();
                    builder.extend(values);
                    Ok(Arc::new(builder.finish()))
                }
                ScalarValue::Int(i) => {
                    let mut builder = Int64Builder::new();
                    let values = (0..len).map(|_| Some(*i)).collect::<Vec<Option<i64>>>();
                    builder.extend(values);
                    Ok(Arc::new(builder.finish()))
                }
                ScalarValue::Utf8(s) => {
                    let mut builder = StringBuilder::new();
                    let values = (0..len)
                        .map(|_| Some(s.clone()))
                        .collect::<Vec<Option<String>>>();
                    builder.extend(values);
                    Ok(Arc::new(builder.finish()))
                }
            }
        }
        Expr::Binary(b) => {
            let left_arr = evaluate_expr(b.left.as_ref(), batch)?;
            let right_arr = evaluate_expr(b.right.as_ref(), batch)?;
            let res: ArrayRef = match b.op {
                Operator::Eq => Arc::new(eq(&left_arr, &right_arr)?),
                Operator::NotEq => Arc::new(neq(&left_arr, &right_arr)?),
                Operator::Lt => Arc::new(lt(&left_arr, &right_arr)?),
                Operator::LtEq => Arc::new(lt_eq(&left_arr, &right_arr)?),
                Operator::Gt => Arc::new(gt(&left_arr, &right_arr)?),
                Operator::GtEq => Arc::new(gt_eq(&left_arr, &right_arr)?),
                Operator::Plus => Arc::new(add(&left_arr, &right_arr)?),
                Operator::Minus => Arc::new(sub(&left_arr, &right_arr)?),
                Operator::Multiply => Arc::new(mul(&left_arr, &right_arr)?),
                Operator::Divide => Arc::new(div(&left_arr, &right_arr)?),
                Operator::And => {
                    Arc::new(and_kleene(left_arr.as_boolean(), right_arr.as_boolean())?)
                }
                Operator::Or => Arc::new(or_kleene(left_arr.as_boolean(), right_arr.as_boolean())?),
            };
            Ok(res)
        }
        Expr::Function(_) => {
            let name = expr.to_string();
            let col = batch.schema().index_of(&name)?;
            Ok(batch.column(col).clone())
        }
    }
}

fn merge_schema(left: &Schema, right: &Schema) -> arrow::datatypes::SchemaRef {
    let mut fields = left.fields().to_vec();
    fields.extend(right.fields().iter().cloned());
    Arc::new(Schema::new(fields))
}

pub trait Executor {
    fn next(&mut self) -> Option<Result<RecordBatch, ArrowError>>;
}

struct ScanExec {
    reader: csv::Reader<File>,
    table_name: String,
}

impl Executor for ScanExec {
    fn next(&mut self) -> Option<Result<RecordBatch, ArrowError>> {
        let Some(Ok(batch)) = self.reader.next() else {
            return None;
        };

        let table = &self.table_name;
        let old_schema = batch.schema();

        let new_fields: Vec<Field> = old_schema
            .fields()
            .iter()
            .map(|f| {
                Field::new(
                    &format!("{}.{}", table, f.name()),
                    f.data_type().clone(),
                    f.is_nullable(),
                )
            })
            .collect();

        let new_schema = Arc::new(Schema::new(new_fields));
        Option::from(RecordBatch::try_new(new_schema, batch.columns().to_vec()))
    }
}

struct ProjectExec {
    child: Box<dyn Executor>,
    expressions: Vec<Expr>,
}

impl Executor for ProjectExec {
    fn next(&mut self) -> Option<Result<RecordBatch, ArrowError>> {
        loop {
            let mut arrays = Vec::with_capacity(self.expressions.len());
            let mut fields = Vec::with_capacity(self.expressions.len());
            let batch = self.child.next()?.unwrap();
            if batch.num_rows() == 0 {
                continue;
            }
            for expr in &self.expressions {
                let arr = evaluate_expr(expr, &batch).unwrap();
                let name = match expr {
                    Expr::Alias(a) => get_column_name(&a.name, &a.table_name),
                    Expr::Column(c) => get_column_name(&c.name, &c.table_name),
                    Expr::Literal(_) => unreachable!(),
                    Expr::Binary(_) => unreachable!(),
                    Expr::Function(_) => unreachable!(),
                };
                println!("Adding field: {}", &name);
                let data_type = arr.data_type().clone();
                fields.push(Field::new(&name, data_type, false));
                arrays.push(arr);
            }

            let schema = Arc::new(Schema::new(fields));
            return Some(RecordBatch::try_new(schema, arrays));
        }
    }
}

struct FilterExec {
    child: Box<dyn Executor>,
    predicate: Expr,
}

impl Executor for FilterExec {
    fn next(&mut self) -> Option<Result<RecordBatch, ArrowError>> {
        loop {
            let batch = self.child.next();
            batch.as_ref()?;
            let batch = batch.unwrap().unwrap();
            if batch.num_rows() == 0 {
                continue;
            }
            let predicate_arr = evaluate_expr(&self.predicate, &batch).unwrap();
            let filtered_res = filter_record_batch(&batch, predicate_arr.as_boolean()).unwrap();
            if filtered_res.num_rows() > 0 {
                return Some(Ok(filtered_res));
            }
        }
    }
}

struct JoinExec {
    left: Box<dyn Executor>,
    right: Box<dyn Executor>,
    predicate: Expr,
    finished: bool,
}

impl Executor for JoinExec {
    fn next(&mut self) -> Option<ArrowResult<RecordBatch>> {
        if self.finished {
            return None;
        }
        self.finished = true;

        let mut left_batches = Vec::new();
        while let Some(Ok(batch)) = self.left.next() {
            left_batches.push(batch);
        }

        let mut output_batches = Vec::new();
        while let Some(Ok(right_batch)) = self.right.next() {
            let m = right_batch.num_rows();
            if m == 0 {
                continue;
            }

            for left_batch in &left_batches {
                let n = left_batch.num_rows();
                if n == 0 {
                    continue;
                }

                let mut left_idx_builder = UInt32Builder::new();
                for _ in 0..m {
                    for i in 0..n {
                        left_idx_builder.append_value(i as u32);
                    }
                }
                let left_idx = left_idx_builder.finish();

                let mut right_idx_builder = UInt32Builder::new();
                for j in 0..m {
                    for _ in 0..n {
                        right_idx_builder.append_value(j as u32);
                    }
                }
                let right_idx = right_idx_builder.finish();

                let mut cols = Vec::with_capacity(n * m);
                for col in left_batch.columns() {
                    cols.push(take(col, &left_idx, None).unwrap());
                }
                for col in right_batch.columns() {
                    cols.push(take(col, &right_idx, None).unwrap());
                }

                let schema =
                    merge_schema(left_batch.schema().as_ref(), right_batch.schema().as_ref());

                let combined = RecordBatch::try_new(schema.clone(), cols).unwrap();
                let mask = evaluate_expr(&self.predicate, &combined).ok()?;

                let filtered = filter_record_batch(&combined, mask.as_boolean()).unwrap();
                if filtered.num_rows() > 0 {
                    output_batches.push(filtered);
                }
            }
        }

        if output_batches.is_empty() {
            let empty = RecordBatch::new_empty(Arc::new(Schema::empty()));
            return Some(Ok(empty));
        }
        let final_batch = concat_batches(&output_batches[0].schema(), &output_batches).unwrap();
        Some(Ok(final_batch))
    }
}

pub struct SortExec {
    child: Box<dyn Executor>,
    exprs: Vec<SortExpr>,
    finished: bool,
}

impl Executor for SortExec {
    fn next(&mut self) -> Option<Result<RecordBatch, ArrowError>> {
        if self.finished {
            return None;
        }
        self.finished = true;

        let mut batches = Vec::new();
        while let Some(Ok(batch)) = self.child.next() {
            batches.push(batch);
        }
        if batches.is_empty() {
            return None;
        }
        let schema = batches[0].schema();
        let combined = concat_batches(&schema, &batches).unwrap();

        let mut cols = Vec::with_capacity(self.exprs.len());

        for sort_expr in &self.exprs {
            let arr = evaluate_expr(&sort_expr.expr, &combined).unwrap();
            let opts = SortOptions {
                descending: !sort_expr.asc,
                nulls_first: false,
            };
            cols.push(SortColumn {
                values: arr,
                options: Some(opts),
            });
        }

        let indices = if cols.len() == 1 {
            sort_to_indices(&cols[0].values, Some(cols[0].options.unwrap()), None).unwrap()
        } else {
            lexsort_to_indices(&cols, None).unwrap()
        };

        let mut sorted_cols = Vec::with_capacity(combined.num_columns());
        for col in combined.columns() {
            sorted_cols.push(take(col.as_ref(), &indices, None).unwrap());
        }

        let sorted = RecordBatch::try_new(schema.clone(), sorted_cols).unwrap();
        Some(Ok(sorted))
    }
}
