WITH category AS (
    SELECT * FROM {{ source("views_orders",'category')}}
),
order_raw AS (
    SELECT * FROM {{ source('views_orders','orders')}}
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