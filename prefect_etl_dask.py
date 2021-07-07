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
# Dask
from dask.distributed import Client
import dask.dataframe as dd

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

