{{ config(materialized = 'view', schema = env_var('DBT_SALESMARTSCHEMA', 'SALESMART')) }}

select * from
{{ref('trf_products')}}