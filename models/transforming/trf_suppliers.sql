{{config(materialized = 'table', schema = env_var('DBT_TRANSFORMSCHEMA', 'TRANSFORMING'))}}
 
select
GET(XMLGET(SupplierInfo, 'SupplierID'), '$') as supplierid,
GET(XMLGET(SupplierInfo, 'CompanyName'), '$')::varchar as companyname,
GET(XMLGET(SupplierInfo, 'ContactName'), '$')::varchar as contactname,
GET(XMLGET(SupplierInfo, 'Address'), '$')::varchar  as address,
GET(XMLGET(SupplierInfo, 'City'), '$')::varchar  as city,
GET(XMLGET(SupplierInfo, 'PostalCode'), '$')::varchar  as postalcode,
GET(XMLGET(SupplierInfo, 'Country'), '$')::varchar  as country,
GET(XMLGET(SupplierInfo, 'Phone'), '$')::varchar  as phone,
GET(XMLGET(SupplierInfo, 'Fax'), '$')::varchar  as fax,
from
{{ref('stg_suppliers')}}