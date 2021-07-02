import time
import numpy as np
import pandas as pd
# S3 & Postgres
import psycopg2
import boto3
import s3fs
from sqlalchemy import create_engine
# Prefect
from prefect import task, Flow, Parameter
from datetime import date, timedelta
from prefect.schedules import Schedule, IntervalSchedule
from prefect.schedules.clocks import CronClock
from prefect.engine.state import Success, Failed
from prefect.utilities.notifications import slack_notifier
# local
from credentials import *

# 1. have docker running (prefect uses docker-compose)
# 2. 'pip install prefect'
# 3. 'prefect backend server'
# 4. 'prefect server start' (default http://localhost:8080)
# 5. 'prefect agent local start'
# 6. `prefect create project '<project_name>'`
# 7. register and run the flow in the python script with the tasks
# 8. set up retries, slack notifications, etc

# TODO: Dask deployment: https://docs.prefect.io/core/advanced_tutorials/dask-cluster.html
# everything looks the same except:
# from prefect.executors import DaskExecutor
# executor = DaskExecutor(address="<tcp_address_of_scheduler>:<port>")
# flow.run(executor=executor)

handler = slack_notifier(only_states=[Success, Failed])

@task
def generate_daily_data(rows):
    """
    generate upsert-able (some new rows, some replacing) data for daily insertion.
    I am using all zeros to make the new rows easily identifiable.
    we can use original data for benchmarking; daily data doesn't need to be big.
    so we generate 100 new rows with index random between 0 and 40 million
    """
    
    idx = [ np.random.randint(0,40000000) for _ in range(rows) ]
    
    df = pd.DataFrame(np.zeros(shape=(rows, 20))).reset_index()
    df.columns = ['ID', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J',
                   'Z', 'Y', 'X', 'W', 'V', 'U', 'T', 'S', 'R', 'Q']
    
    df.index = idx
    df['ID'] = idx

    print(df.head())

    df.to_csv(f'data/new_data_{date.today()}.csv')
    print(f"Data for {date.today()} saved")

    return df

@task
def to_s3(file, s3file):
    """upload a file to the s3 mnnk/dsca/ folder"""
    
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file(file, 'mnnk', f'dsca/{s3file}')

    print(f"{s3file} saved to S3")

@task
def from_s3(s3filepath, cols, sep=','):
    """turn s3 file into pandas dataframe"""
    filetype = s3filepath[-3:]

    if filetype == 'csv':
        try:
            df = pd.read_csv(s3filepath, sep=sep, usecols=cols)
        except:
            print("ERROR")
    elif filetype == 'fwf':
        try:
            df = pd.read_fwf(s3filepath)
        except:
            print("ERROR")
    
    return df

@task
def upsert_postgres(df, table, cols, pk, max_retries=1, retry_delay=timedelta(minutes=1), state_handlers=[handler]):
    """
    "Merge" new data into existing Postgres database, 
    replacing old data when a new row ID matches an
    existing one
    """
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

    # Index(['ID', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'Z', 'Y', 'X', 'W', 'V', 'U', 'T', 'S', 'R', 'Q']

    # string of cols in quotes separated by comma and space
    sql_cols = ', '.join(list(map(lambda x: '"' + x + '"', cols)))
    sql_pk = '"' + pk + '"'
    
    # change column names to fit any DataFrame
    sql = f""" 
            INSERT INTO {table} ({sql_cols})
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

    # commit and close
    conn.commit()
    conn.close()

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
    return df1.merge(df2, left_on=col1, right_on=col2)

def sort(df, by, asc=True):
    return df.sort_values(by=by, ascending=asc)

def main():
    # design the flow
    with Flow("DSCA ETL") as flow:
        # parameters
        cols = Parameter("cols", 
                    default=['ID', 'A', 'B', 'C', 'D', 'E', 'F', 
                            'G', 'H', 'I', 'J', 'Z', 'Y', 'X', 
                            'W', 'V', 'U', 'T', 'S', 'R', 'Q'])
        file = Parameter("file", default=f'data/new_data_{date.today()}.csv')
        s3file = Parameter("s3file", default=f'new_data_{date.today()}.csv')
        s3filepath = Parameter("s3filepath", default=f'{S3_PATH}/new_data_{date.today()}.csv')
        sep = Parameter("sep", default=',')
        pk = Parameter("pk", default='ID')


        # generate new data
        generate_daily_data(100)

        # dump into S3 bucket
        to_s3(f'data/new_data_{date.today()}.csv', f'new_data_{date.today()}.csv')

        # retreive the new data from S3
        df = from_s3(f'{S3_PATH}/new_data_2021-06-29.csv', cols)\
            
        # upsert the new data into postgres
        upsert_postgres(df, 'newtable', cols, 'ID')


    projname = "dsca_etl"

    # schedule flow
    schedule = Schedule(clocks=[CronClock("40 21 * * *")])
    flow.schedule = schedule

    # register flow
    flow.register(project_name=projname)
    print("Flow registered")

    # execute the flow on the specified schedule
    #flow.run()

if __name__ == "__main__":
    main()


