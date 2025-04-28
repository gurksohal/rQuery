# SQL Query Engine
Mini SQL query engine built on top of apache arrow

## Supported Features
- Scans (csv)
- Projections
- Filters
- Sorts
- Joins
- Basic expressions and aliases
- Filter pushdown
- Create and copy tables (see `data\load.sql`)

## Example query to logical plan:
### Query
```sql
SELECT customers.name, products.category, sales.quantity
FROM sales,
     customers,
     products
WHERE sales.customer_id = customers.customer_id
  AND sales.product_id = products.product_id
  AND products.category = 'Electronics'
order by customers.name, products.category, sales.quantity
```

### Logical plan:
```text
└── Sort: [customers.name ASC, products.category ASC, sales.quantity ASC]
    └── Projection: [customers.name, products.category, sales.quantity]
        └── Join: [(sales.product_id == products.product_id)]
            ├── Join: [(sales.customer_id == customers.customer_id)]
            │   ├── Scan: sales
            │   └── Scan: customers
            └── Filter: [(products.category == Electronics)]
                └── Scan: products

```

## Usage
- `cargo run`

### Available commands inside the CLI:
  - `load` - Loads sample data from `data/load.sql`
  - `q <N>` - Executes a predefined SQL file located at `data/queries/N.sql` (e.g., q 1, q 2 up to q 5).
  - `<SQL STMT>` - Execute arbitrary SQL statement
  - `exit`

### Schema of sample data
```sql
TABLE customers
(
    customer_id INTEGER     NOT NULL,
    name        VARCHAR(30) NOT NULL,
    region      VARCHAR(30) NOT NULL
);

TABLE products
(
    product_id INTEGER     NOT NULL,
    name       VARCHAR(30) NOT NULL,
    category   VARCHAR(30) NOT NULL
);

TABLE sales
(
    sale_id     INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    product_id  INTEGER NOT NULL,
    quantity    INTEGER NOT NULL,
    price       INTEGER NOT NULL,
    is_returned BOOLEAN NOT NULL
);
```

## Verify output
- Run `cargo run --features="build-binary" --bin df_compare` to compare output for the predefined queries with datafusion

## Todo
- Add support for 'group by'
- Hash join
- simple query optimizer (beyond just basic filter pushdown)
- intra query parallelism (via exchange operators?)
- tpc-h at some point?