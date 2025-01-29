def model(dbt, session):
    stg_products = dbt.source("qwt_raw", "raw_products")
    return stg_products