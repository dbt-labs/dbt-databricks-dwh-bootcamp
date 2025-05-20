WITH customer_bronze AS (
  SELECT
    *
  FROM {{ source('tpch', 'customer_bronze') }}
), filter_1 AS (
  SELECT
    c_custkey,
    c_name,
    c_nationkey,
    c_acctbal
  FROM customer_bronze
  WHERE
    NOT c_custkey IS NULL AND c_nationkey <> 21 AND c_nationkey <> 22
), customer_silver_sql AS (
  SELECT
    *
  FROM filter_1
)
SELECT
  *
FROM customer_silver_sql