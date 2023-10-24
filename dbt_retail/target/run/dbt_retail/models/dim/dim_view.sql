
  create or replace   view VIEWS_ORDERS.DEV.dim_view
  
   as (
    

WITH  __dbt__cte__src_view as (
WITH category AS (
    SELECT * FROM VIEWS_ORDERS.raw.product_category
),
view_raw AS (
    SELECT * FROM VIEWS_ORDERS.raw.view_data
)
SELECT DISTINCT
view_raw.event,
view_raw.messageid,
view_raw.userid,
view_raw.productid,
view_raw.source,
category.categoryid,
view_raw.timestamp
FROM view_raw 
LEFT JOIN category 
ON view_raw.productid = category.productid
),src_view AS (
    SELECT * FROM __dbt__cte__src_view
)
SELECT 
COUNT(productid) as view_count,
productid,
categoryid as category,
ROW_NUMBER() OVER (PARTITION BY categoryid ORDER BY COUNT(productid) DESC) AS RN
FROM src_view 
GROUP BY productid, categoryid
  );

