import time
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import boto3
import s3fs
from prefect import Flow
from credentials import *
from generate import generate_daily_data
from etl_funcs import to_s3, from_s3, connect_postgres, upsert_postgres, create_and_fill_table_postgres, set_pk_postgres, merge, sort

"""
1. generate new daily data (already merged into final postgres format)
2. dump it into s3
3. pull from s3 and sort
4. upsert to postgres
"""

if __name__ == "__main__":

    colnames = ['ID', 'A', 'B', 'C', 'D', 'E', 'F', 
                'G', 'H', 'I', 'J', 'Z', 'Y', 'X', 
                'W', 'V', 'U', 'T', 'S', 'R', 'Q']

    df = from_s3(f'{S3_PATH}/new_data_2021-06-29.csv', cols=colnames)

    conn, engine = connect_postgres() # connect to postgres database

    #create_and_fill_table_postgres(engine=engine, df=df, table='newtable')
    #set_pk_postgres(conn=conn, engine=engine, table='newtable', pk='ID')
    start_time = time.time()
    upsert_postgres(df, 'newtable', engine, colnames, 'ID')
    print(f"Action took {(time.time() - start_time)} seconds")

    conn.commit() # commit to database
    print("Changes committed")
    conn.close() # close connection
    print("Connection closed")