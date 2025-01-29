import snowflake.snowpark.functions as f
import pandas as pd 
import holidays

def is_holiday(date_col):
    french_holidays = holidays.France()
    is_holiday = (date_col in french_holidays)
    return is_holiday

def avg_order_values(sales,orders):

    return sales/orders

def model(dbt, session):
    dbt.config(materialized = 'table', schema = 'reporting', packages = 'holidays')

    dim_customers_df = dbt.ref("dim_customers")
    fct_orders_df = dbt.ref("fct_orders")

    customer_orders_df = (
        fct_orders_df
        .group_by('customerid')
        .agg(
            f.min(f.col('orderdate')).alias('first_order_date'),
            f.max(f.col('orderdate')).alias('recent_order_date'),
            f.count(f.col('orderid')).alias('total_orders'),
            f.sum(f.col('LineSalesAmount')).alias('total_sales')

        )
    )

    final_df = (dim_customers_df.join(customer_orders_df,dim_customers_df.customerid == customer_orders_df.customerid,'left')
    .select(
        dim_customers_df.customerid.alias('customerid'),
        dim_customers_df.companyname.alias('companyname'),
        dim_customers_df.contactname.alias('contactname'),
        customer_orders_df.first_order_date.alias('first_order_date'),
        customer_orders_df.recent_order_date.alias('recent_order_date'),
        customer_orders_df.total_orders.alias('total_orders'),
        customer_orders_df.total_sales.alias('total_sales')
    )
)

    #adding new column with use of withColumn funtion and calling average_order_value funtion
    final_df = final_df.withColumn("average_order_values", avg_order_values(final_df["total_sales"], final_df["total_orders"] ))
    #removing null values rows in the data
    final_df = final_df.filter(f.col("FIRST_ORDER_DATE").isNotNull())

    final_df = final_df.to_pandas()

    final_df["IS_FIRSTORDERDATE_ON_HOLIDAY"] = final_df["FIRST_ORDER_DATE"].apply(is_holiday)

    return final_df
