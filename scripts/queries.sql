-- ============================================================
-- MSBA 305 — Analytical SQL Queries
-- Olist E-Commerce PostgreSQL Pipeline
-- ============================================================

-- 1. Top 10 Product Categories by Total Revenue
-- Identifies product categories generating the highest revenue.
SELECT
    pc.product_category_name_english AS product_category,
    SUM(oi.price + oi.freight_value) AS total_revenue
FROM orders AS o
JOIN order_items AS oi
    ON o.order_id = oi.order_id
JOIN products AS p
    ON oi.product_id = p.product_id
JOIN product_categories AS pc
    ON p.product_category_name = pc.product_category_name
GROUP BY pc.product_category_name_english
ORDER BY total_revenue DESC
LIMIT 10;


-- 2. Monthly Order Volume and Total Revenue Over Time
-- Shows monthly order count and total revenue.
SELECT
    TO_CHAR(o.order_purchase_timestamp, 'YYYY-MM') AS order_month,
    COUNT(o.order_id) AS total_orders,
    SUM(o.total_order_value) AS total_revenue
FROM orders AS o
GROUP BY TO_CHAR(o.order_purchase_timestamp, 'YYYY-MM')
ORDER BY order_month;


-- 3. Average Review Score by Customer State
-- Identifies regional differences in customer satisfaction.
SELECT
    c.customer_state,
    AVG(orr.review_score) AS average_review_score,
    COUNT(o.order_id) AS total_orders_reviewed
FROM orders AS o
JOIN customers AS c
    ON o.customer_id = c.customer_id
JOIN order_reviews AS orr
    ON o.order_id = orr.order_id
WHERE orr.review_score IS NOT NULL
GROUP BY c.customer_state
ORDER BY average_review_score DESC
LIMIT 10;


-- 4. Order Volume on Holidays vs. Non-Holidays
-- Compares order volume and average order value on holidays and non-holidays.
SELECT
    CASE
        WHEN h.holiday_name IS NOT NULL THEN 'Holiday'
        ELSE 'Non-Holiday'
    END AS day_type,
    COUNT(o.order_id) AS total_orders,
    AVG(o.total_order_value) AS average_order_value
FROM orders AS o
LEFT JOIN holidays AS h
    ON DATE(o.order_purchase_timestamp) = h.holiday_date
GROUP BY day_type
ORDER BY total_orders DESC;


-- 5. Average Delivery Time Per Seller (Top 10 Sellers by Volume)
-- Compares average delivery time for the top 10 sellers by order volume.
WITH TopSellers AS (
    SELECT
        oi.seller_id,
        COUNT(o.order_id) AS seller_order_count
    FROM orders AS o
    JOIN order_items AS oi
        ON o.order_id = oi.order_id
    GROUP BY oi.seller_id
    ORDER BY seller_order_count DESC
    LIMIT 10
)
SELECT
    s.seller_id,
    s.seller_city,
    AVG(o.delivery_days) AS avg_delivery_days,
    TS.seller_order_count
FROM orders AS o
JOIN order_items AS oi
    ON o.order_id = oi.order_id
JOIN sellers AS s
    ON oi.seller_id = s.seller_id
JOIN TopSellers AS TS
    ON s.seller_id = TS.seller_id
WHERE
    o.is_delivered = TRUE
    AND o.delivery_days IS NOT NULL
GROUP BY
    s.seller_id,
    s.seller_city,
    TS.seller_order_count
ORDER BY avg_delivery_days ASC;
