{{config(materialized = 'table', schema = env_var('DBT_TRANSFORMSCHEMA', 'TRANSFORMING'))}}
 
select
sh.orderid,
sh.lineno,
sp.companyname,
sh.shipmentdate,
sh.status
from
{{ref('shipments_snapshot')}}  as sh left join {{ref('lkp_shippers')}} as sp
on sh.shipperid = sp.shipperid
where sh.dbt_valid_to is null
