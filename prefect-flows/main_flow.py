import duckdb
import os
from dotenv import load_dotenv
import pandas as pd
from prefect import task, flow
import requests
from query import *
from transform_queries import *
load_dotenv()

@task(name='motherduck-connection')
def md_conn():
    md_token = os.getenv('md_token_new')
    con = duckdb.connect("md:product_sales_db", config={"motherduck_token": md_token})
    return con

@task(name='create-tables')
def create_tables(con):
    create_queries = [
        create_t_cast_types,
        create_final_product_sales,
        create_dim_order_dates,
        create_dim_payment_methods,
        create_dim_products,
        create_dim_customers,
        create_final_fact_sales,
        create_mart_product_sales
        ]
    for query in create_queries:
        try:
            con.sql(query)
        except Exception as e:
            print(f"Error occured at {query}: {e}")

@task(name='fact-mart-tables')
def insert_values(con):
    sql_queries = [
        t_cast_types,
        final_product_sales,
        dim_order_dates,
        dim_stores,
        dim_payment_methods,
        dim_products,
        dim_customers,
        final_fact_sales,
        mart_product_sales
        ]
    
    for query in sql_queries:
        try:
            con.sql(query)
        except Exception as e:
            print(f"Error occured at {query}: {e}")

@flow(name='run-transformations')
def main():
    con = md_conn()
    create_tables(con)
    insert_values(con)

if __name__ == '__main__':
    main()