# Setup
1. have docker running (prefect uses docker-compose)
2. `pip install prefect`
3. `prefect backend server`
4. `prefect server start` (default http://localhost:8080)
5. `prefect agent local start`
6. `prefect create project '<project_name>'`
7. register and run the flow in the python script with the tasks
8. set up retries, slack notifications, etc

# Dask deployment
https://docs.prefect.io/core/advanced_tutorials/dask-cluster.html
Everything looks the same except: 
```
from prefect.executors import DaskExecutor
executor = DaskExecutor(address="<tcp_address_of_scheduler>:<port>")
flow.run(executor=executor)
```