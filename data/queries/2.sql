SELECT sales.sale_id, sales.quantity * sales.price AS total_price
FROM sales
WHERE sales.is_returned = false
order by sales.sale_id