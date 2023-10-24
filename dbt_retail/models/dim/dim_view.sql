{{
  config(
    materialized = 'view'
  )
}}

WITH src_view AS (
    SELECT * FROM {{ ref("src_view")}}
)
SELECT 
COUNT(productid) as view_count,
productid,
categoryid as category,
ROW_NUMBER() OVER (PARTITION BY categoryid ORDER BY COUNT(productid) DESC) AS RN
FROM src_view 
GROUP BY productid, categoryid

