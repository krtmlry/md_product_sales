import duckdb
import io
import os
from dotenv import load_dotenv
import pandas as pd
from prefect import task, flow
import requests
from query import *
load_dotenv()

@task(name='get-raw-csv')
def raw_csv():
    response = requests.get(os.getenv('csv_url'))
    raw_sales_df = pd.read_csv(io.StringIO(response.text), sep=',')
    raw_sales_df = pd.DataFrame(raw_sales_df)
    return raw_sales_df

@task(name='get-states-data')
def states_data():
    response = requests.get(os.getenv('states_url'))
    states_df = pd.read_csv(io.StringIO(response.text),sep=',')
    states_df = pd.DataFrame(states_df)
    return states_df

@task(name='motherduck-connection')
def md_conn():
    md_token = os.getenv('md_token')
    con = duckdb.connect("md:product_sales_db", config={"motherduck_token": md_token})
    return con

@task(name='load-to-motherduck')
def load_to_md(con,raw_sales_df,states_df):
    con.sql(trunc_tables)
    con.sql(insert_raw_sales)
    con.sql(insert_raw_dim_states)
    con.sql(trans_dim_states)

@flow(name='extract-load-motherduck')
def main():
    raw_sales_df = raw_csv()
    states_df = states_data()
    con = md_conn()
    load_to_md(con,raw_sales_df,states_df)

if __name__ == '__main__':
    main()