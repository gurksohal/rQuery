CREATE TABLE customers
(
    customer_id INTEGER     NOT NULL,
    name        VARCHAR(30) NOT NULL,
    region      VARCHAR(30) NOT NULL
);

CREATE TABLE products
(
    product_id INTEGER     NOT NULL,
    name       VARCHAR(30) NOT NULL,
    category   VARCHAR(30) NOT NULL
);

CREATE TABLE sales
(
    sale_id     INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    product_id  INTEGER NOT NULL,
    quantity    INTEGER NOT NULL,
    price       INTEGER NOT NULL,
    is_returned BOOLEAN NOT NULL
);

COPY customers FROM 'data/tables/customers.csv' (DELIMITER ',', HEADER false);
COPY products FROM 'data/tables/products.csv' (DELIMITER ',', HEADER false);
COPY sales FROM 'data/tables/sales.csv' (DELIMITER ',', HEADER false);
