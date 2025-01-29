{{config(materialized = 'table', schema = env_var('DBT_TRANSFORMSCHEMA', 'TRANSFORMING'), transient = false)}}

select 
p.productid
, p.productname
, s.companyname as suppliercompany
, s.ContactName as suppliercontact
, s.City as suppliercity
, s.Country as suppliercountry
, c.categoryname
, p.unitcost
, p.unitprice
, p.unitsinstock
, p.unitsonorder
, TO_DECIMAL(p.unitprice - p.unitcost, 5,2) as profit
, iff(p.unitsonorder > unitsinstock, 'Not Avilable', 'Avilable') as productavailablity
from
{{ref('stg_products')}} as p 
left join {{ref('trf_suppliers')}} as s on p.supplierid = s.supplierid
left join {{ref('lkp_categories')}} as c on p.categoryid = c.categoryid