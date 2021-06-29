import time
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import boto3
import s3fs
from credentials import *

# The original insert of 30M rows worked but it took hours...probably 8-12 hrs

def to_s3(file, s3name):
    """upload a file to the s3 mnnk/dsca/ folder"""
    
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file(file, 'mnnk', f'dsca/{s3name}')

    print(f"{s3name} saved to S3")

def from_s3(s3file, cols, sep=','):
    """turn s3 file into pandas dataframe"""
    filetype = s3file[-3:]

    if filetype == 'csv':
        try:
            df = pd.read_csv(s3file, sep=sep, usecols=cols)
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

def upsert_postgres(df, table, engine, colnames, pk):
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
                           "A" = EXCLUDED."A",
                           "B" = EXCLUDED."B",
                           "C" = EXCLUDED."C",
                           "D" = EXCLUDED."D",
                           "E" = EXCLUDED."E",
                           "F" = EXCLUDED."F",
                           "G" = EXCLUDED."G",
                           "H" = EXCLUDED."H",
                           "I" = EXCLUDED."I",
                           "J" = EXCLUDED."J",
                           "Z" = EXCLUDED."Z",
                           "Y" = EXCLUDED."Y",
                           "X" = EXCLUDED."X",
                           "W" = EXCLUDED."W",
                           "V" = EXCLUDED."V",
                           "U" = EXCLUDED."U",
                           "T" = EXCLUDED."T",
                           "S" = EXCLUDED."S",
                           "R" = EXCLUDED."R",
                           "Q" = EXCLUDED."Q";
        """

    # execute upsert
    engine.execute(sql)

def create_and_fill_table_postgres(engine, df, table):
    """save pandas dataframe to a new postgres table with a primary key"""
    # insert data
    try:
        start_time = time.time()
        df.to_sql(table, con=engine, if_exists='replace', chunksize=20000, method='multi')
        print(f"to_sql took {(time.time() - start_time)} seconds")
        print(f"{table} created and filled")
    except:
        print("DataFrame format doesn't match database table format")

def set_pk_postgres(conn, engine, table, pk):
    """set primary key (otherwise upsert won't work)"""
    
    engine.execute(f'ALTER TABLE {table} ADD CONSTRAINT "{pk}" PRIMARY KEY ("{pk}");')

    print(f"{pk} set to Primary Key")

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

    colnames = ['ID', 'A', 'B', 'C', 'D', 'E', 'F', 
                'G', 'H', 'I', 'J', 'Z', 'Y', 'X', 
                'W', 'V', 'U', 'T', 'S', 'R', 'Q']

    df = from_s3(f'{S3_PATH}/new_data_2021-06-29.csv', cols=colnames)
    print(df.head())

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

