{{config(materialized = 'view', schema = 'reporting')}}
 
select
c.companyname, c.contactname, c.city,
min(o.orderdate) as first_order_date, max(o.orderdate) as recent_order_date, sum(o.quantity) as total_orders,
count(p.productid) as total_products, sum(o.linesalesamount) as total_sales, avg(o.margin) as avg_margin
from
{{ref('fct_orders')}} as o inner join {{ref('dim_customers')}} as c on c.customerid = o.customerid
inner join {{ref('dim_products')}} as p on p.productid = o.productid
group by c.companyname, c.contactname, c.city
order by total_sales desc