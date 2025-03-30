use crate::db::TableMetadata;
use crate::logical_plan::Expr::{Binary, Literal};
use crate::logical_plan::Operator::{
    And, Divide, Eq, Gt, GtEq, Lt, LtEq, Minus, Multiply, NotEq, Or, Plus,
};
use sqlparser::ast;
use sqlparser::ast::{
    BinaryOperator, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, OrderByKind,
    Query, SelectItem, SetExpr, TableFactor, Value,
};
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct Scan {
    pub table_name: String,
}

#[derive(Clone, Debug)]
pub struct Projection {
    pub expr: Vec<Expr>,
    pub input: Arc<LogicalPlan>,
}

#[derive(Clone, Debug)]
pub struct Filter {
    pub predicate: Expr,
    pub input: Arc<LogicalPlan>,
}

#[derive(Clone, Debug)]
pub struct Aggregate {
    pub input: Arc<LogicalPlan>,
    pub group_by_expr: Vec<Expr>,
    pub aggregate_expr: Vec<Expr>,
}

#[derive(Clone, Debug)]
pub struct SortExpr {
    pub expr: Expr,
    pub asc: bool,
}

#[derive(Clone, Debug)]
pub struct Sort {
    pub expr: Vec<SortExpr>,
    pub input: Arc<LogicalPlan>,
}

#[derive(Clone, Debug)]
pub struct Join {
    pub left: Arc<LogicalPlan>,
    pub right: Arc<LogicalPlan>,
    pub on: Vec<Expr>,
}

#[derive(Clone, Debug)]
pub struct Alias {
    pub expr: Box<Expr>,
    pub table_name: Option<String>,
    pub name: String,
}

#[derive(Clone, Debug)]
pub struct Column {
    pub table_name: Option<String>,
    pub name: String,
}

#[derive(Clone, Debug)]
pub struct BinaryExpr {
    pub left: Box<Expr>,
    pub op: Operator,
    pub right: Box<Expr>,
}

#[derive(Clone, Debug)]
pub struct Function {
    pub name: AggregateFunction,
    pub args: Vec<Expr>,
}

#[derive(Clone, Debug)]
pub enum Operator {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Plus,
    Minus,
    Multiply,
    Divide,
    And,
    Or,
}

#[derive(Clone, Debug)]
pub enum ScalarValue {
    Boolean(bool),
    Int(i64),
    Utf8(String),
}

#[derive(Clone, Debug)]
pub enum AggregateFunction {
    Sum,
    Count,
    Min,
    Max,
    Avg,
}

#[derive(Clone, Debug)]
pub enum LogicalPlan {
    Scan(Scan),
    Projection(Projection),
    Filter(Filter),
    Aggregate(Aggregate),
    Sort(Sort),
    Join(Join),
}

#[derive(Clone, Debug)]
pub enum Expr {
    Alias(Alias),
    Column(Column),
    Literal(ScalarValue),
    Binary(BinaryExpr),
    Function(Function),
}

impl LogicalPlan {
    pub fn build_logical_plan(query_stmt: Query) -> Arc<Self> {
        let SetExpr::Select(select) = *query_stmt.body else {
            panic!("Unsupported query body: {:?}", &query_stmt.body);
        };
        assert!(select.distinct.is_none(), "DISTINCT not supported");
        assert!(select.into.is_none(), "INTO not supported");
        assert!(select.having.is_none(), "HAVING not supported");

        // FROM
        assert!(!select.from.is_empty(), "FROM requires at least one table");
        let table_names: Vec<String> = select
            .from
            .iter()
            .map(|twf| {
                assert!(twf.joins.is_empty(), "JOIN in FROM clause not supported");
                let TableFactor::Table { name, .. } = &twf.relation else {
                    panic!("Only base table supported");
                };
                name.to_string()
            })
            .collect();
        let mut plan = Self::create_scan_joins(table_names);

        // WHERE
        if let Some(predicate) = &select.selection {
            let logical_expr = Self::create_logical_expr(predicate);
            plan = LogicalPlan::Filter(Filter {
                predicate: logical_expr,
                input: Arc::new(plan),
            });
        }

        // GROUP BY / Aggregates
        let has_func = select.projection.iter().any(|item| {
            let e = match item {
                SelectItem::UnnamedExpr(expr) => Self::create_logical_expr(expr),
                SelectItem::ExprWithAlias { expr, .. } => Self::create_logical_expr(expr),
                _t => panic!("not implemented projection: {:?}", _t),
            };

            matches!(e, Expr::Function(_))
        });

        let GroupByExpr::Expressions(expressions, expressions_mod) = &select.group_by else {
            panic!("GROUP BY only supports expressions");
        };
        assert!(
            expressions_mod.is_empty(),
            "GROUP BY with expression modifiers not supported"
        );
        let group_expressions: Vec<Expr> =
            expressions.iter().map(Self::create_logical_expr).collect();

        // PROJECTION
        let mut projection_expr = vec![];
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    projection_expr.push(Self::create_logical_expr(expr));
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let e = Self::create_logical_expr(expr);
                    projection_expr.push(Expr::Alias(Alias {
                        expr: e.into(),
                        table_name: None,
                        name: alias.value.clone(),
                    }));
                }
                _t => panic!("not implemented projection: {:?}", _t),
            }
        }

        if has_func || !group_expressions.is_empty() {
            let functions = projection_expr
                .iter()
                .filter(|e| match e {
                    Expr::Alias(Alias { expr, .. }) => matches!(expr.as_ref(), Expr::Function(_)),
                    Expr::Function(_) => true,
                    _ => false,
                })
                .map(|e| {
                    if let Expr::Alias(Alias { expr, .. }) = e {
                        expr
                    } else {
                        e
                    }
                })
                .cloned()
                .collect::<Vec<Expr>>();
            plan = LogicalPlan::Aggregate(Aggregate {
                input: Arc::new(plan),
                group_by_expr: group_expressions,
                aggregate_expr: functions,
            });
        }

        plan = LogicalPlan::Projection(Projection {
            expr: projection_expr,
            input: Arc::new(plan),
        });

        // SORT
        if let Some(order_by) = &query_stmt.order_by {
            assert_eq!(order_by.interpolate, None);
            let OrderByKind::Expressions(expressions) = &order_by.kind else {
                panic!("Only ORDER BY expressions supported");
            };

            let e = expressions
                .iter()
                .map(|o_expr| {
                    let asc = o_expr.options.asc.unwrap_or(true);
                    SortExpr {
                        expr: Self::create_logical_expr(&o_expr.expr),
                        asc,
                    }
                })
                .collect::<Vec<SortExpr>>();

            plan = LogicalPlan::Sort(Sort {
                expr: e,
                input: Arc::new(plan),
            });
        }

        plan.into()
    }

    fn create_scan_joins(table_names: Vec<String>) -> Self {
        let mut plan_nodes: Vec<LogicalPlan> = table_names
            .iter()
            .map(|name| {
                LogicalPlan::Scan(Scan {
                    table_name: name.to_string(),
                })
            })
            .collect();

        while plan_nodes.len() > 1 {
            plan_nodes = plan_nodes
                .chunks(2)
                .map(|chunk| {
                    if chunk.len() != 2 {
                        return chunk[0].clone();
                    }
                    let left = Arc::new(chunk[0].clone());
                    let right = Arc::new(chunk[1].clone());
                    LogicalPlan::Join(Join {
                        left,
                        right,
                        on: vec![],
                    })
                })
                .collect();
        }

        assert_eq!(plan_nodes.len(), 1);
        plan_nodes.pop().unwrap()
    }

    fn create_logical_expr(predicate: &ast::Expr) -> Expr {
        match predicate {
            ast::Expr::Identifier(ident) => Expr::Column(Column {
                table_name: None,
                name: ident.value.clone(),
            }),
            ast::Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                assert!(!negated, "NOT BETWEEN not supported");
                let low_expr = Self::create_logical_expr(low);
                let high_expr = Self::create_logical_expr(high);
                let logical_expr = Self::create_logical_expr(expr);
                let lt_eq = BinaryExpr {
                    left: logical_expr.clone().into(),
                    op: GtEq,
                    right: low_expr.into(),
                };
                let gt_eq = BinaryExpr {
                    left: logical_expr.into(),
                    op: LtEq,
                    right: high_expr.into(),
                };

                Binary(BinaryExpr {
                    left: Binary(lt_eq).into(),
                    op: Eq,
                    right: Binary(gt_eq).into(),
                })
            }
            ast::Expr::BinaryOp { left, op, right } => Binary(BinaryExpr {
                left: Self::create_logical_expr(left).into(),
                op: Self::create_binary_op(op),
                right: Self::create_logical_expr(right).into(),
            }),
            ast::Expr::Nested(e) => Self::create_logical_expr(e),
            ast::Expr::Value(vs) => Literal(Self::create_scale_value(&vs.value)),
            ast::Expr::Function(func) => {
                assert_eq!(
                    func.name.0.len(),
                    1,
                    "function name object parts must only be length 1 got: {:?}",
                    func.name.0
                );
                let func_name = func
                    .name
                    .0
                    .first()
                    .unwrap()
                    .as_ident()
                    .unwrap()
                    .value
                    .clone();
                let aggregate_func = match func_name.to_lowercase().as_str() {
                    "sum" => AggregateFunction::Sum,
                    "count" => AggregateFunction::Count,
                    "min" => AggregateFunction::Min,
                    "max" => AggregateFunction::Max,
                    "avg" => AggregateFunction::Avg,
                    _t => panic!("func not supported: {_t}"),
                };

                let FunctionArguments::List(func_args_list) = &func.args else {
                    panic!("func args must be a list");
                };
                let all_inputs: Vec<Expr> = func_args_list
                    .args
                    .iter()
                    .map(|arg| match arg {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                            Self::create_logical_expr(expr)
                        }
                        _ => panic!("only unnamed args supported"),
                    })
                    .collect();
                assert_eq!(all_inputs.len(), 1, "Expected only 1 arg per function");

                Expr::Function(Function {
                    name: aggregate_func,
                    args: all_inputs,
                })
            }
            ast::Expr::CompoundIdentifier(idents) => {
                assert_eq!(idents.len(), 2, "Compound identifiers must have 2 part");
                let table_name = idents[0].value.clone();
                let column_name = idents[1].value.clone();
                Expr::Column(Column {
                    table_name: Some(table_name),
                    name: column_name,
                })
            }
            _t => panic!("not implemented expr: {:?}", _t),
        }
    }

    fn create_binary_op(op: &BinaryOperator) -> Operator {
        match op {
            BinaryOperator::Plus => Plus,
            BinaryOperator::Minus => Minus,
            BinaryOperator::Multiply => Multiply,
            BinaryOperator::Divide => Divide,
            BinaryOperator::Gt => Gt,
            BinaryOperator::Lt => Lt,
            BinaryOperator::GtEq => GtEq,
            BinaryOperator::LtEq => LtEq,
            BinaryOperator::Eq => Eq,
            BinaryOperator::NotEq => NotEq,
            BinaryOperator::And => And,
            BinaryOperator::Or => Or,
            _t => panic!("not implemented op: {:?}", _t),
        }
    }

    fn create_scale_value(value: &Value) -> ScalarValue {
        match value {
            Value::Number(string_value, _) => ScalarValue::Int(string_value.parse().unwrap()),
            Value::SingleQuotedString(s) => ScalarValue::Utf8(s.clone()),
            Value::DoubleQuotedString(s) => ScalarValue::Utf8(s.clone()),
            Value::Boolean(b) => ScalarValue::Boolean(*b),
            _t => panic!("not implemented value: {:?}", _t),
        }
    }

    fn children(&self) -> Vec<Arc<Self>> {
        match self {
            Self::Scan(_) => vec![],
            Self::Projection(projection) => vec![projection.input.clone()],
            Self::Filter(filter) => vec![filter.input.clone()],
            Self::Aggregate(aggregate) => vec![aggregate.input.clone()],
            Self::Sort(sort) => vec![sort.input.clone()],
            Self::Join(join) => vec![join.left.clone(), join.right.clone()],
        }
    }

    fn fmt(f: &mut Formatter<'_>, plan: &Self, indent: &str, is_last: bool) -> std::fmt::Result {
        let branch = if is_last { "└── " } else { "├── " };
        write!(f, "{}{}", indent, branch)?;
        let new_indent = format!("{}{}", indent, if is_last { "    " } else { "│   " });

        match plan {
            Self::Scan(scan) => {
                writeln!(f, "Scan: {}", scan.table_name)?;
            }
            Self::Join(join) => {
                writeln!(
                    f,
                    "Join: [{}]",
                    join.on
                        .iter()
                        .map(|o| o.to_string())
                        .collect::<Vec<String>>()
                        .join(", ")
                )?;
                Self::fmt(f, &join.left, &new_indent, false)?;
                Self::fmt(f, &join.right, &new_indent, true)?;
            }
            Self::Projection(projection) => {
                writeln!(
                    f,
                    "Projection: [{}]",
                    projection
                        .expr
                        .iter()
                        .map(|o| o.to_string())
                        .collect::<Vec<String>>()
                        .join(", ")
                )?;
                Self::fmt(f, &projection.input, &new_indent, true)?;
            }
            Self::Filter(filter) => {
                writeln!(f, "Filter: [{}]", filter.predicate)?;
                Self::fmt(f, &filter.input, &new_indent, true)?;
            }
            Self::Aggregate(aggregate) => {
                writeln!(
                    f,
                    "Aggregate: group_by: [{}], aggregates: [{}]",
                    aggregate
                        .group_by_expr
                        .iter()
                        .map(|o| o.to_string())
                        .collect::<Vec<String>>()
                        .join(", "),
                    aggregate
                        .aggregate_expr
                        .iter()
                        .map(|o| o.to_string())
                        .collect::<Vec<String>>()
                        .join(", ")
                )?;
                Self::fmt(f, &aggregate.input, &new_indent, true)?;
            }
            Self::Sort(sort) => {
                writeln!(
                    f,
                    "Sort: [{}]",
                    sort.expr
                        .iter()
                        .map(|o| o.expr.to_string() + if o.asc { " ASC" } else { " DESC" })
                        .collect::<Vec<String>>()
                        .join(", ")
                )?;
                Self::fmt(f, &sort.input, &new_indent, true)?;
            }
        }

        Ok(())
    }
}

//TODO: remove later whenever i decide to add optimizer
impl LogicalPlan {
    pub fn rewrite(node: Arc<Self>, catalog: &HashMap<String, TableMetadata>) -> Arc<Self> {
        // Resolve table names for each col
        let p =
            LogicalPlan::apply_top_down(node, &|node| Self::set_column_table_names(node, catalog));

        // Push down filters
        LogicalPlan::apply_top_down(p, &Self::filter_push_down)
    }

    fn filter_push_down(node: Arc<Self>) -> Arc<Self> {
        let LogicalPlan::Filter(f) = node.as_ref() else {
            return node;
        };

        match f.input.as_ref() {
            LogicalPlan::Scan(_) => node,
            LogicalPlan::Projection(p) => {
                let new_filter = LogicalPlan::Filter(Filter {
                    predicate: f.predicate.clone(),
                    input: p.input.clone(),
                });

                LogicalPlan::Projection(Projection {
                    expr: p.expr.clone(),
                    input: new_filter.into(),
                })
                .into()
            }
            LogicalPlan::Filter(child_f) => {
                let combined_pred =
                    Self::combine_ands(vec![child_f.predicate.clone(), f.predicate.clone()]);
                LogicalPlan::Filter(Filter {
                    predicate: combined_pred,
                    input: child_f.input.clone(),
                })
                .into()
            }
            LogicalPlan::Aggregate(_) => node,
            LogicalPlan::Sort(s) => {
                let new_filter = LogicalPlan::Filter(Filter {
                    predicate: f.predicate.clone(),
                    input: s.input.clone(),
                });
                LogicalPlan::Sort(Sort {
                    expr: s.expr.clone(),
                    input: new_filter.into(),
                })
                .into()
            }
            LogicalPlan::Join(j) => {
                let mut predicates = Self::split_ands(&f.predicate);

                let left_tables = Self::collect_tables(j.left.clone());
                let right_tables = Self::collect_tables(j.right.clone());
                let both_tables: HashSet<String> =
                    left_tables.union(&right_tables).cloned().collect();

                let mut left_preds = vec![];
                let mut right_preds = vec![];
                let mut join_preds = vec![];

                predicates.retain(|p| {
                    let table_names = Self::expr_table_names(&p);
                    if table_names.is_subset(&left_tables) {
                        left_preds.push(p.clone());
                        false
                    } else if table_names.is_subset(&right_tables) {
                        right_preds.push(p.clone());
                        false
                    } else if table_names.is_subset(&both_tables) {
                        join_preds.push(p.clone());
                        false
                    } else {
                        true
                    }
                });

                let new_left = if left_preds.is_empty() {
                    j.left.clone()
                } else {
                    LogicalPlan::Filter(Filter {
                        predicate: Self::combine_ands(left_preds),
                        input: j.left.clone(),
                    })
                    .into()
                };

                let new_right = if right_preds.is_empty() {
                    j.right.clone()
                } else {
                    LogicalPlan::Filter(Filter {
                        predicate: Self::combine_ands(right_preds),
                        input: j.right.clone(),
                    })
                    .into()
                };

                let new_join = LogicalPlan::Join(Join {
                    left: new_left,
                    right: new_right,
                    on: join_preds,
                });

                if predicates.is_empty() {
                    return new_join.into();
                }

                LogicalPlan::Filter(Filter {
                    predicate: Self::combine_ands(predicates),
                    input: new_join.into(),
                })
                .into()
            }
        }
    }

    fn collect_tables(node: Arc<Self>) -> HashSet<String> {
        match node.as_ref() {
            LogicalPlan::Scan(s) => {
                let mut tables = HashSet::new();
                tables.insert(s.table_name.clone());
                tables
            }
            LogicalPlan::Projection(p) => Self::collect_tables(p.input.clone()),
            LogicalPlan::Filter(f) => Self::collect_tables(f.input.clone()),
            LogicalPlan::Aggregate(a) => Self::collect_tables(a.input.clone()),
            LogicalPlan::Sort(s) => Self::collect_tables(s.input.clone()),
            LogicalPlan::Join(j) => {
                let mut tables = Self::collect_tables(j.left.clone());
                tables.extend(Self::collect_tables(j.right.clone()));
                tables
            }
        }
    }

    fn set_column_table_names(
        node: Arc<Self>,
        catalog: &HashMap<String, TableMetadata>,
    ) -> Arc<Self> {
        match node.as_ref() {
            LogicalPlan::Scan(_) => node,
            LogicalPlan::Projection(p) => {
                let e = p
                    .expr
                    .iter()
                    .map(|e| Self::set_table_names(e, catalog))
                    .collect::<Vec<Expr>>();
                LogicalPlan::Projection(Projection {
                    expr: e,
                    input: p.input.clone(),
                })
                .into()
            }
            LogicalPlan::Filter(f) => LogicalPlan::Filter(Filter {
                predicate: Self::set_table_names(&f.predicate, catalog),
                input: f.input.clone(),
            })
            .into(),
            LogicalPlan::Aggregate(a) => {
                let group_by = a
                    .group_by_expr
                    .iter()
                    .map(|e| Self::set_table_names(e, catalog))
                    .collect::<Vec<Expr>>();
                let aggregate = a
                    .aggregate_expr
                    .iter()
                    .map(|e| Self::set_table_names(e, catalog))
                    .collect::<Vec<Expr>>();
                LogicalPlan::Aggregate(Aggregate {
                    group_by_expr: group_by,
                    aggregate_expr: aggregate,
                    input: a.input.clone(),
                })
                .into()
            }
            LogicalPlan::Sort(s) => {
                let e = s
                    .expr
                    .iter()
                    .map(|e| SortExpr {
                        expr: Self::set_table_names(&e.expr, catalog),
                        asc: e.asc,
                    })
                    .collect::<Vec<SortExpr>>();
                LogicalPlan::Sort(Sort {
                    expr: e,
                    input: s.input.clone(),
                })
                .into()
            }
            LogicalPlan::Join(j) => {
                let e =
                    j.on.iter()
                        .map(|o| Self::set_table_names(o, catalog))
                        .collect::<Vec<Expr>>();
                LogicalPlan::Join(Join {
                    left: j.left.clone(),
                    right: j.right.clone(),
                    on: e,
                })
                .into()
            }
        }
    }

    fn set_table_names(expr: &Expr, catalog: &HashMap<String, TableMetadata>) -> Expr {
        match expr {
            Expr::Alias(a) => Expr::Alias(Alias {
                expr: Self::set_table_names(&a.expr, catalog).into(),
                table_name: a.table_name.clone(),
                name: a.name.clone(),
            }),
            Expr::Column(c) => {
                let c_name = &c.name;
                if c.table_name.is_some() {
                    return expr.clone();
                }
                let t: Option<(&String, &TableMetadata)> = catalog.iter().find(|&entry| {
                    let schema = entry.1.schema.clone();
                    schema.fields.iter().any(|f| f.name() == c_name)
                });

                if let Some((table_name, _)) = t {
                    Expr::Column(Column {
                        table_name: Some(table_name.clone()),
                        name: c_name.clone(),
                    })
                } else {
                    Expr::Column(Column {
                        table_name: None,
                        name: c_name.clone(),
                    })
                }
            }
            Literal(_) => expr.clone(),
            Binary(b) => Binary(BinaryExpr {
                left: Self::set_table_names(&b.left, catalog).into(),
                right: Self::set_table_names(&b.right, catalog).into(),
                op: b.op.clone(),
            }),
            Expr::Function(f) => Expr::Function(Function {
                name: f.name.clone(),
                args: f
                    .args
                    .iter()
                    .map(|a| Self::set_table_names(a, catalog))
                    .collect(),
            }),
        }
    }

    fn expr_table_names(expr: &Expr) -> HashSet<String> {
        match expr {
            Expr::Alias(a) => Self::expr_table_names(a.expr.as_ref()),
            Expr::Column(c) => HashSet::from([c.table_name.clone().unwrap()]),
            Literal(_) => HashSet::new(),
            Binary(b) => {
                let mut left_names = Self::expr_table_names(b.left.as_ref());
                let right_names = Self::expr_table_names(b.right.as_ref());
                left_names.extend(right_names);
                left_names
            }
            Expr::Function(f) => {
                let mut args_names = HashSet::new();
                for arg in f.args.iter() {
                    args_names.extend(Self::expr_table_names(arg));
                }
                args_names
            }
        }
    }

    fn apply_top_down(node: Arc<Self>, f: &impl Fn(Arc<Self>) -> Arc<Self>) -> Arc<Self> {
        let node = f(node.clone());
        let mut children = vec![];
        for child in node.children() {
            let child = Self::apply_top_down(child, f);
            children.push(child);
        }

        Self::create_node_with_children(&node, children).into()
    }

    // fn apply_bottom_up(node: Arc<Self>, f: &impl Fn(Arc<Self>) -> Arc<Self>) -> Arc<Self> {
    //     let mut children = vec![];
    //     for child in node.children() {
    //         let child = Self::apply_bottom_up(child, f);
    //         children.push(child);
    //     }
    // 
    //     let node = Arc::new(Self::create_node_with_children(&node, children));
    //     f(node.clone())
    // }

    fn split_ands(predicate: &Expr) -> Vec<Expr> {
        if let Binary(BinaryExpr {
            left,
            op: Operator::And,
            right,
        }) = predicate
        {
            let mut left = Self::split_ands(left);
            let mut right = Self::split_ands(right);
            left.append(&mut right);
            left
        } else {
            vec![predicate.clone()]
        }
    }

    fn combine_ands(mut predicates: Vec<Expr>) -> Expr {
        let first = predicates.remove(0);
        predicates.into_iter().fold(first, |acc, p| {
            Binary(BinaryExpr {
                left: acc.into(),
                op: And,
                right: p.into(),
            })
        })
    }

    fn create_node_with_children(node: &Self, children: Vec<Arc<Self>>) -> Self {
        match node {
            LogicalPlan::Scan(s) => {
                assert_eq!(children.len(), 0, "Scan node must have no children");
                LogicalPlan::Scan(s.clone())
            }
            LogicalPlan::Projection(p) => {
                assert_eq!(children.len(), 1, "Projection node must have 1 child");
                LogicalPlan::Projection(Projection {
                    expr: p.expr.clone(),
                    input: children[0].clone(),
                })
            }
            LogicalPlan::Filter(f) => {
                assert_eq!(children.len(), 1, "Filter node must have 1 child");
                LogicalPlan::Filter(Filter {
                    predicate: f.predicate.clone(),
                    input: children[0].clone(),
                })
            }
            LogicalPlan::Aggregate(a) => {
                assert_eq!(children.len(), 1, "Aggregate node must have 1 child");
                LogicalPlan::Aggregate(Aggregate {
                    input: children[0].clone(),
                    group_by_expr: a.group_by_expr.clone(),
                    aggregate_expr: a.aggregate_expr.clone(),
                })
            }
            LogicalPlan::Sort(s) => {
                assert_eq!(children.len(), 1, "Sort node must have 1 child");
                LogicalPlan::Sort(Sort {
                    expr: s.expr.clone(),
                    input: children[0].clone(),
                })
            }
            LogicalPlan::Join(j) => {
                assert_eq!(children.len(), 2, "Join node must have 2 children");
                LogicalPlan::Join(Join {
                    left: children[0].clone(),
                    right: children[1].clone(),
                    on: j.on.clone(),
                })
            }
        }
    }
}

impl Display for Expr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Expr::Alias(alias) => {
                write!(f, "{} AS {}", alias.expr, alias.name)
            }
            Expr::Column(column) => {
                if column.table_name.is_none() {
                    return write!(f, "{}", column.name);
                }
                write!(
                    f,
                    "{}.{}",
                    column.table_name.as_ref().unwrap_or(&"".to_string()),
                    column.name
                )
            }
            Literal(v) => match v {
                ScalarValue::Boolean(b) => write!(f, "{}", b),
                ScalarValue::Int(b) => write!(f, "{}", b),
                ScalarValue::Utf8(b) => write!(f, "{}", b),
            },
            Binary(bin) => write!(f, "({} {} {})", bin.left, bin.op, bin.right),
            Expr::Function(func) => {
                let func_name = match func.name {
                    AggregateFunction::Sum => "sum",
                    AggregateFunction::Count => "count",
                    AggregateFunction::Min => "min",
                    AggregateFunction::Max => "max",
                    AggregateFunction::Avg => "avg",
                };
                write!(
                    f,
                    "{}({})",
                    func_name,
                    func.args
                        .iter()
                        .map(|o| o.to_string())
                        .collect::<Vec<String>>()
                        .join(", ")
                )
            }
        }
    }
}

impl Display for Operator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Eq => write!(f, "=="),
            NotEq => write!(f, "!="),
            Lt => write!(f, "<"),
            LtEq => write!(f, "<="),
            Gt => write!(f, ">"),
            GtEq => write!(f, ">="),
            Plus => write!(f, "+"),
            Minus => write!(f, "-"),
            Multiply => write!(f, "*"),
            Divide => write!(f, "/"),
            And => write!(f, "AND"),
            Or => write!(f, "OR"),
        }
    }
}

impl Display for LogicalPlan {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Self::fmt(f, self, "", true)
    }
}
