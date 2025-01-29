{{ config(materialized = 'view', schema = 'salesmart') }}
 
{% set min_order_date = get_min_orderdate() %}
{% set max_order_date = get_max_orderdate() %}  

{{ dbt_date.get_date_dimension(min_order_date,max_order_date)}}