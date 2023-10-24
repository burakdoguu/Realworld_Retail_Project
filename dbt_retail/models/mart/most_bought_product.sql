{{ config(
  materialized = 'table',
) }}

/*WITH dim_view AS (
    SELECT * FROM {{ ref("dim_view")}}
)*/
WITH dim_product AS (
    SELECT 
    total_bought,
    productid,
    category,
    TRY_CAST(REGEXP_SUBSTR(category, '[0-9]+') AS INT) as extracted_number
    FROM {{ ref("dim_product")}}
    WHERE RN <= 10
    ORDER BY 4
)

SELECT total_bought,productid,category FROM dim_product