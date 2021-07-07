import time
import numpy as np
import pandas as pd
# S3 & Postgres
import psycopg2
import boto3
import s3fs
from sqlalchemy import create_engine
# local
from credentials import *
# Dask
from dask.distributed import Client
import dask.dataframe as dd

"""
Benchmark / compare the following use cases for Pandas vs. Dask dataframes:
1. read_csv
2. merge
3. sort
4. to_postgres

Most performant:
1. dd.read_csv
2/3. 

Where can we explicitly parallelize (with dask.delayed)?
"""

# Pandas

"""
# read_csv
start_time = time.time()
df1 = pd.read_csv('data/numbers1_30000000.csv', index_col=0)
df2 = pd.read_csv('data/numbers2_30000000.csv', index_col=0)
print(f"Pandas took {time.time() - start_time} seconds to read in the dataframes")


# merge
start_time = time.time()
df = df1.merge(df2, left_on='ID', right_on='ID')
print(f"Pandas took {time.time() - start_time} seconds to merge the dataframes")

# sort
start_time = time.time()
df = df.sort_values(by='ID')
print(f"Pandas took {time.time() - start_time} seconds to sort the dataframes")
"""

# Dask
#client = Client(n_workers=4)

# read_csv
start_time = time.time()
df3 = dd.read_csv('data/numbers1_30000000.csv')
df4 = dd.read_csv('data/numbers2_30000000.csv')
df3 = df3.loc[:, ~df3.columns.str.contains('^Unnamed')]
df4 = df4.loc[:, ~df4.columns.str.contains('^Unnamed')]
print(f"Dask took {time.time() - start_time} seconds to read in the dataframes")

# sort
start_time = time.time()
df_sort3 = df3.set_index('ID')
df_sort4 = df4.set_index('ID')
print(f"Dask took {time.time() - start_time} seconds to set indexes")

# merge
start_time = time.time()
df = df_sort3.merge(df_sort4, left_index=True, right_index=True).compute()
print(df.head())
print(f"Dask took {time.time() - start_time} seconds to merge the dataframes")

"""
# merge (this converts it to a pandas dataframe apparently?)
# large-to-large unsorted joins are slow
# then sort using nlargest
start_time = time.time()
df = df3.merge(df4, left_on='ID', right_on='ID').compute()
print(f"Dask took {time.time() - start_time} seconds to merge the dataframes (unsorted)")

start_time = time.time()
df_sort2 = df.nlargest(len(df.index), 'ID')
print(df_sort2.head())
print(f"Dask took {time.time() - start_time} seconds to sort the dataframes with nlargest")


# sort (sorting in parallel is different; have to set index or use nlargest)
start_time = time.time()
df_sort3 = df3.set_index('ID')
df_sort4 = df4.set_index('ID')
print(df_sort3.head())
print(df_sort4.head())
print(f"Dask took {time.time() - start_time} seconds to set index of both dataframes")

# merge the dataframes with the new index A
start_time = time.time()
df_merged = df_sort3.merge(df_sort4, left_index=True, right_index=True)
print(df.head())
print(f"Dask took {time.time() - start_time} seconds to merge the dataframes (by index)")
"""

#client.shutdown()

