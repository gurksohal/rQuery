SELECT customers.name, products.category, sales.quantity
FROM sales,
     customers,
     products
WHERE sales.customer_id = customers.customer_id
  AND sales.product_id = products.product_id
  AND products.category = 'Electronics'
order by sales.quantity