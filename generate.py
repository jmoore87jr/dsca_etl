import pandas as pd 
import numpy as np
import random

# generate initial data
# DSAMS: 
#  columns: ID1, ID2, ID3, col1, col2, col3,...col7
# CISIL: 
#  columns: ID, col1, col2, col3,...col9

# generate new smaller data daily

def generate_initial_data(rows):
    """
    generate two dataframes about 1GB in size each, joinable on ID column
    """
    df1 = pd.DataFrame(np.random.randint(0,100,size=(rows, 10))).reset_index()
    df1.columns = ['ID', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J']

    # shuffle ID column
    df1 = df1.sample(frac=1)

    df1.to_csv(f'data/numbers_{rows}.csv')

    print(f"Dataframe with {rows} saved")

    df2 = pd.DataFrame(np.random.randint(0,100,size=(rows, 10))).reset_index()
    df2.columns = ['ID', 'Z', 'Y', 'X', 'W', 'V', 'U', 'T', 'S', 'R', 'Q']

    # shuffle ID column
    df2 = df2.sample(frac=1)

    df2.to_csv(f'data/numbers_{rows}.csv')

    print(f"Dataframe with {rows} saved")



def generate_daily_data():
    """
    generate upsert-able (some new rows, some replacing) data for daily insertion
    """
    pass

def to_s3(csvs):
    """
    put the new csvs in S3
    """
    pass


generate_initial_data(30000000)