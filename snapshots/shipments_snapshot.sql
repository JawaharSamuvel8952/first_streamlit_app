{% snapshot shipments_snapshot %}
{{
    config
    (
        target_database = env_var('DBT_DBNAME', 'QWTANALYTICS'),
        target_schema = env_var('DBT_SNAPSHOTSCHEMA', 'SNAPSHOTS'),
        unique_key = "orderid||'-'||lineno",

        strategy = 'timestamp',
        updated_at = 'shipmentdate'
    )
}}

select * from {{ref('stg_shipments')}}

{% endsnapshot %}