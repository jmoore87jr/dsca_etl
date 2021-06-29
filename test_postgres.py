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

def connect_postgres():
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

    except psycopg2.OperationalError as e:
        print(e)

    return conn, engine

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

def upsert(df, table, engine, colnames, pk):
    """
    "Merge" new data into existing Postgres database, 
    replacing old data when a new row ID matches an
    existing one
    """

    # Index(['ID', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'Z', 'Y', 'X', 'W', 'V', 'U', 'T', 'S', 'R', 'Q']

    # string of colnames in quotes separated by comma and space
    sql_colnames = ', '.join(list(map(lambda x: '"' + x + '"', colnames)))
    sql_pk = '"' + pk + '"'
    
    # change column names to fit any DataFrame
    sql = f""" 
            INSERT INTO {table} ({sql_colnames})
            VALUES {','.join([str(i) for i in list(df.to_records(index=False))])}
            ON CONFLICT ({sql_pk})
            DO  
                UPDATE SET "ID" = EXCLUDED."ID",
                           "Item" = EXCLUDED."Item",
                           "Date" = EXCLUDED."Date",
                           "Date2" = EXCLUDED."Date2",
                           "Date3" = EXCLUDED."Date3";
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
            start_time = time.time()
            df.to_sql(table, con=engine, if_exists='replace', chunksize=20000, method='multi')
            print(f"to_sql took {(time.time() - start_time)} seconds")
            print("Data inserted")
        except:
            print("Data format doesn't match database format")

    except psycopg2.OperationalError as e:
        print(e)

def set_pk(conn, engine, table, pk):
    """set primary key (otherwise upsert won't work)"""
    
    engine.execute(f'ALTER TABLE {table} ADD CONSTRAINT "{pk}" PRIMARY KEY ("{pk}");')

    print("ID altered to Primary Key")


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

if __name__ == "__main__":

    conn, engine = connect_postgres() # connect to postgres database

    set_pk(conn=conn, engine=engine, table='newtable', pk='ID')

    conn.commit() # commit to database
    conn.close() # close connection

    #df1 = pd.read_csv('data/numbers1_30000000.csv').drop(columns=['Unnamed: 0'])
    #df2 = pd.read_csv('data/numbers2_30000000.csv').drop(columns=['Unnamed: 0'])


    #df = merge(df1, df2, "ID", "ID")
    #df = sort(df, by="ID", asc=True)


    #initialize_postgres(df, table='newtable', pk='ID')


# THIS WORKS
"""
df = pd.DataFrame({'col1': [3, 4, 5], 'col2': [6, 7, 8]})
initialize_postgres(df, table='test', pk='col1')
"""
# The original insert of 30M rows worked but it took hours...probably 8-12 hrs

