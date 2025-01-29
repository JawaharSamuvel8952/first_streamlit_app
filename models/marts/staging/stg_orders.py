import snowflake.snowpark.functions as f

def model(dbt, session):
    dbt.config(materialized = "incremental", unique_key = 'orderid')
    df_orders = dbt.source("qwt_raw", "raw_orders")

    if dbt.is_incremental:
        # only new rows compared to max in current table
        max_order_date = f"select max(orderdate) from {dbt.this}"
        df_orders = df_orders.filter(df_orders.orderdate >= session.sql(max_order_date).collect()[0][0])

    return df_orders



