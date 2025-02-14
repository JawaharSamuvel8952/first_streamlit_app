{{config(materialized='table', schema=env_var("DBT_TRANSFORMSCHEMA","TRANSFORMING"))}}

select
id as heros_id,
name,
power as heros_power,
points,
case when points <= 500 then 'weak'
     when points > 500 and points <= 900 then 'heavy'
     when points > 900 then 'legendary' end as status,
power_start_date,
case when  date(power_start_date) > {{dbt.current_timestamp()}} then 'rookie' else 'captain' end as ranks

from {{ref("stg_powers")}}