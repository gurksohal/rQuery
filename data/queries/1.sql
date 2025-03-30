SELECT sales.sale_id
FROM sales
WHERE sales.quantity > 90
  and sales.customer_id = 10
order by sales.sale_id