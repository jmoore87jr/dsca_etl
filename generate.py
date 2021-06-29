import pandas as pd 
import numpy as np
import boto3
import random
from datetime import date
from credentials import *


def to_s3(file, s3name):
    """
    put the new csvs in S3
    """
    
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file(file, 'mnnk', f'dsca/{s3name}')

    print(f"{s3name} saved to S3")


def generate_initial_data(rows):
    """
    generate two dataframes about 1GB in size each, joinable on ID column
    """
    df1 = pd.DataFrame(np.random.randint(0,100,size=(rows, 10))).reset_index()
    df1.columns = ['ID', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J']

    # shuffle ID column
    df1 = df1.sample(frac=1).reset_index(drop=True)

    df1.to_csv(f'data/numbers1_{rows}.csv')

    print(f"Dataframe 1 with {rows} rows saved")

    df2 = pd.DataFrame(np.random.randint(0,100,size=(rows, 10))).reset_index()
    df2.columns = ['ID', 'Z', 'Y', 'X', 'W', 'V', 'U', 'T', 'S', 'R', 'Q']

    # shuffle ID column
    df2 = df2.sample(frac=1).reset_index(drop=True)

    df2.to_csv(f'data/numbers2_{rows}.csv')

    print(f"Dataframe 2 with {rows} rows saved")

    return df1, df2


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


#generate_initial_data(30000000)

#to_s3('data/numbers1_30000000.csv', 'numbers1_30000000.csv')
#to_s3('data/numbers2_30000000.csv', 'numbers2_30000000.csv')

df = generate_daily_data(100)
#to_s3(f'data/new_data_{date.today()}.csv', f'new_data_{date.today()}.csv')
print(df.head())
print(','.join([str(i) for i in list(df.to_records(index=False))]))
