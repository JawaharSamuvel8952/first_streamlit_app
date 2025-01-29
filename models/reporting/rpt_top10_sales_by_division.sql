{{config(materialized = 'view', schema = 'reporting')}}

select * from (
select c.companyname,c.contactname,c.divisionname, sum(linesalesamount) as sales
from {{ref('fct_orders')}} as o
inner join {{ref('dim_customers')}} as c on c.customerid = o.customerid
where divisionname = '{{var('v_division', 'Europe')}}' 
group by c.companyname,c.contactname,c.divisionname
) order by sales desc limit 10