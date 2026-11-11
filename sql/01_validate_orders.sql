SELECT
    order_date,
    COUNT(*) AS valid_rows,
    COUNT(DISTINCT order_id) AS unique_orders,
    ROUND(SUM(quantity * unit_price), 2) AS total_revenue
FROM orders_analytics_db.orders
WHERE order_date = '2026-11-10'
GROUP BY order_date;
