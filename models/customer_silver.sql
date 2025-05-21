WITH customer_bronze AS (
  SELECT
    *
  FROM {{ source('tpch', 'customer_bronze') }}
), filter_3ca6 AS (
  SELECT
    c_custkey,
    c_name,
    c_nationkey,
    c_acctbal
  FROM customer_bronze
  WHERE
    NOT c_custkey IS NULL AND c_nationkey <> 21
), customer_silver AS (
  SELECT
    *
  FROM filter_3ca6
)
SELECT
  *
FROM customer_silver