{{
  config(
    materialized = 'view'
  )
}}

WITH src_order AS (
    SELECT * FROM {{ ref("src_order")}}
)
SELECT 
SUM(quantity) as total_bought,
productid,
categoryid as category,
ROW_NUMBER() OVER (PARTITION BY categoryid ORDER BY COUNT(quantity) DESC) AS RN
FROM src_order 
GROUP BY productid, categoryid

