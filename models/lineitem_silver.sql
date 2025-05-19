select
    l_orderkey,
    l_linenumber,
    l_quantity,
    l_extendedprice,
    l_discount,
    l_tax,
    l_returnflag,
    l_linestatus,
from {{ source("tpch", "lineitem_bronze") }}
where l_orderkey is not null and l_shipdate >= date '1990-01-01'
