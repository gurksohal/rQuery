SELECT customers.name, sales.quantity * sales.price AS total_spent
FROM sales,
     customers
WHERE sales.quantity > 90
  and sales.customer_id = 10
  and sales.customer_id = customers.customer_id
ORDER BY total_spent DESC
