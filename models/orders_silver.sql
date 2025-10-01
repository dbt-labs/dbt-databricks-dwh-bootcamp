select 
* 
from {{ source('tpch', 'orders_bronze') }}
    where o_orderkey is not null
    and o_orderdate >= date '1990-01-01'
