{{ config(materialized = 'table') }}
 
select 
orderid,
lineno,
shipperid,
customerid,
productid,
employeeid,
TO_DATE(SPLIT_PART(shipmentdate,' ',1)) as shipmentdate,
status
from
{{source('qwt_raw','raw_shipments')}}