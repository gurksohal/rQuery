use rand::distr::Alphabetic;
use rand::prelude::IndexedRandom;
use rand::{Rng, rng};

#[derive(Debug)]
struct Sale {
    sale_id: i32,
    customer_id: i32,
    product_id: i32,
    quantity: i32,
    price: i32,
    is_returned: bool,
}

#[derive(Debug)]
struct Customer {
    customer_id: i32,
    name: String,
    region: String,
}

#[derive(Debug)]
struct Product {
    product_id: i32,
    name: String,
    category: String,
}
fn main() {
    let num_sales = 10_000;
    let num_customers = 1_000;
    let num_products = 100;

    let customers = generate_customers(num_customers);
    let products = generate_products(num_products);
    let sales = generate_sales(
        num_sales,
        &customers.iter().map(|c| c.customer_id).collect::<Vec<_>>(),
        &products.iter().map(|p| p.product_id).collect::<Vec<_>>(),
    );

    let sales_table_path = format!("{}/data/tables/sales.csv", env!("CARGO_MANIFEST_DIR"));
    let customers_table_path = format!("{}/data/tables/customers.csv", env!("CARGO_MANIFEST_DIR"));
    let products_table_path = format!("{}/data/tables/products.csv", env!("CARGO_MANIFEST_DIR"));

    let mut sales_writer = csv::Writer::from_path(sales_table_path).unwrap();
    let mut customers_writer = csv::Writer::from_path(customers_table_path).unwrap();
    let mut products_writer = csv::Writer::from_path(products_table_path).unwrap();

    for sale in sales {
        sales_writer
            .write_record(&[
                sale.sale_id.to_string(),
                sale.customer_id.to_string(),
                sale.product_id.to_string(),
                sale.quantity.to_string(),
                sale.price.to_string(),
                sale.is_returned.to_string(),
            ])
            .unwrap();
    }

    for customer in customers {
        customers_writer
            .write_record(&[
                customer.customer_id.to_string(),
                customer.region,
                customer.name.to_string(),
            ])
            .unwrap();
    }

    for product in products {
        let record = vec![
            product.product_id.to_string(),
            product.category,
            product.name.to_string(),
        ];
        products_writer.write_record(&record).unwrap();
    }
}

fn generate_customers(n: usize) -> Vec<Customer> {
    let regions = vec![
        "North America",
        "Europe",
        "Asia",
        "South America",
        "Africa",
        "Oceania",
    ];
    let mut rng = rng();
    (0..n)
        .map(|i| Customer {
            customer_id: i as i32,
            name: random_string(10),
            region: regions[rng.random_range(0..regions.len())].to_string(),
        })
        .collect()
}

fn generate_products(n: usize) -> Vec<Product> {
    let categories = vec!["Electronics", "Clothing", "Books", "Home"];
    let mut rng = rng();
    (0..n)
        .map(|i| Product {
            product_id: i as i32,
            name: random_string(10),
            category: categories[rng.random_range(0..categories.len())].to_string(),
        })
        .collect()
}

fn generate_sales(n: usize, customer_ids: &[i32], product_ids: &[i32]) -> Vec<Sale> {
    let mut rng = rng();
    (0..n)
        .map(|i| Sale {
            sale_id: i as i32,
            customer_id: customer_ids.choose(&mut rng).unwrap().clone(),
            product_id: product_ids.choose(&mut rng).unwrap().clone(),
            quantity: rng.random_range(1..=100),
            price: rng.random_range(10..=500),
            is_returned: rng.random_bool(0.1),
        })
        .collect()
}

fn random_string(len: usize) -> String {
    rng()
        .sample_iter(&Alphabetic)
        .take(len)
        .map(char::from)
        .collect()
}
