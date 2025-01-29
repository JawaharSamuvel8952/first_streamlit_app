select orderid, sum(p.profit) as profit
from 
{{ref('dim_products')}} as p 
inner join {{ref('fct_orders')}} as o
on p.productid = p.productid
group by orderid
having sum(p.profit) < 0
