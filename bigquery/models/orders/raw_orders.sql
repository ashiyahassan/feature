
{{
    config(
        alias ='transformed_orders',
        materialized = 'table'
    )
}}

WITH prepared AS (
    SELECT
        order_id,
        DATE(order_date) AS order_date,
        order_name,
       
       FROM
        {{ source('orders_tbl', 'raw_orders') }}

    ORDER BY
        1
)
SELECT
    *
FROM 
    prepared