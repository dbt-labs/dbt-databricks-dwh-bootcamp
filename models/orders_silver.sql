WITH orders_bronze AS (
  SELECT
    *
  FROM {{ source('tpch', 'orders_bronze') }}
), filter_1 AS (
  /* Filter relevant dates */
  SELECT
    *
  FROM orders_bronze
  WHERE
    NOT o_orderkey IS NULL AND o_order_date >= CAST('1990-01-01' AS DATE)
), orders_silver AS (
  SELECT
    *
  FROM filter_1
)
SELECT
  *
FROM orders_silver