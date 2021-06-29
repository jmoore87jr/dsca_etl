import pandas as pd
from prefect import Flow
from generate import generate_daily_data
from etl_funcs import to_s3, from_s3, to_postgres, merge, sort

"""
1. generate new daily data (already merged into final postgres format)
2. dump it into s3
3. pull from s3 and sort
4. upsert to postgres
"""
