SELECT customers.name, sales.quantity
FROM sales,
     customers
WHERE sales.customer_id = customers.customer_id
  AND customers.region = 'North America'
order by sales.quantity
