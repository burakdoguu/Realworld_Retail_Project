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