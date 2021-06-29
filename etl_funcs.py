import time
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import boto3
import s3fs
from credentials import *

def to_s3(file, s3name):
    """upload a file to the s3 mnnk/dsca/ folder"""
    
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file(file, 'mnnk', f'dsca/{s3name}')

    print(f"{s3name} saved to S3")

def from_s3(s3file, sep=','):
    """turn s3 file into pandas dataframe"""
    filetype = s3file[-3:]

    if filetype == 'csv':
        try:
            df = pd.read_csv(s3file, sep=sep)
        except:
            print("ERROR")
    elif filetype == 'fwf':
        try:
            df = pd.read_fwf(s3file)
        except:
            print("ERROR")
    
    return df

def create_table(df, table, conn, engine):
    """
    Create our table in Postgres
    Change column names to match your data
    """

    engine.execute(f"DROP TABLE IF EXISTS {table}")

    sql = f"""
    CREATE TABLE {table} (
        "ID" INTEGER UNIQUE,
        "Item" VARCHAR,
        "Date" VARCHAR,
        "Date2" VARCHAR,
        "Date3" VARCHAR
    );
    """

    # execute upsert
    engine.execute(sql)


def initialize_postgres(df, table, pk):
    """save pandas dataframe to a new postgres table with a primary key"""
    try:
        # connect to database
        conn = psycopg2.connect(
            host=DATABASE_ENDPOINT,
            database=DATABASE_NAME,
            user=USERNAME,
            password=PASSWORD,
            port=PORT
        )
        print("Connected to database")

        # create engine
        engine = create_engine(f'postgresql://{USERNAME}:{PASSWORD}@{DATABASE_ENDPOINT}:{PORT}/{DATABASE_NAME}')

        print("Engine created")

        # insert data
        try:
            df.to_sql(table, con=engine, if_exists='replace', chunksize=20000)
            print("Data inserted")
        except:
            print("Data format doesn't match database format")
        
        # make ID column PRIMARY KEY
        engine.execute(f'ALTER TABLE {table} ADD CONSTRAINT "{pk}" PRIMARY KEY ("{pk}");')

        print("ID altered to Primary Key")

        # commit to database
        conn.commit()
        print("Table created")

        # close connection
        conn.close()
        print("Connection closed")

    except psycopg2.OperationalError as e:
        print(e)

def to_postgres(df, table):
    """upsert pandas dataframe to postgres database"""
    pass

def merge(df1, df2, col1, col2):
    """merge on ID column and time it"""
    start_time = time.time()
    df = df1.merge(df2, left_on=col1, right_on=col2)
    print(f"Merge took {(time.time() - start_time)} seconds")
    
    return df

def sort(df, by, asc=True):
    """sort by ID and time it"""
    start_time = time.time()
    df = df.sort_values(by=by, ascending=asc)
    print(f"Sort took {(time.time() - start_time)} seconds")

    return df


df1 = pd.read_csv('s3://mnnk/dsca/numbers1_30000000.csv')
df2 = pd.read_csv('s3://mnnk/dsca/numbers2_30000000.csv')

df = merge(df1, df2, "ID", "ID")
df = sort(df, by="ID", asc=True)

print(df.head())
print(df.columns)

initialize_postgres(df, table="table1", pk="ID")


