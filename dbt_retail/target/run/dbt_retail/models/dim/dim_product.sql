
  create or replace   view VIEWS_ORDERS.DEV.dim_product
  
   as (
    

WITH  __dbt__cte__src_order as (
WITH category AS (
    SELECT * FROM VIEWS_ORDERS.raw.product_category
),
order_raw AS (
    SELECT * FROM VIEWS_ORDERS.raw.order_data
)
SELECT DISTINCT
order_raw.event,
order_raw.messageid,
order_raw.userid,
order_raw.orderid,
order_raw.productid,
order_raw.quantity,
category.categoryid,
order_raw.timestamp
FROM order_raw 
LEFT JOIN category 
ON order_raw.productid = order_raw.productid
),src_order AS (
    SELECT * FROM __dbt__cte__src_order
)
SELECT 
SUM(quantity) as total_bought,
productid,
categoryid as category,
ROW_NUMBER() OVER (PARTITION BY categoryid ORDER BY COUNT(quantity) DESC) AS RN
FROM src_order 
GROUP BY productid, categoryid
  );

