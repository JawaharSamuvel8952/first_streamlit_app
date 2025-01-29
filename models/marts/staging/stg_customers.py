def model(dbt, session):
    stg_customers = dbt.source("qwt_raw", "raw_customers")
    return stg_customers