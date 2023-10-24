

/*WITH dim_view AS (
    SELECT * FROM VIEWS_ORDERS.DEV.dim_view
)*/
WITH dim_view AS (
    SELECT 
    view_count,
    productid,
    category,
    TRY_CAST(REGEXP_SUBSTR(category, '[0-9]+') AS INT) as extracted_number
    FROM VIEWS_ORDERS.DEV.dim_view
    WHERE RN <= 10
    ORDER BY 4
)

SELECT view_count,productid,category FROM dim_view