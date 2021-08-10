# pipe_final_deepika

data_ingestion.py --> pulled data from api, transformed it into useable json format, pushed it to hdfs.

dags --> dags and transformation applied to the data

Covid --> output from airflow

Documentation:
https://docs.google.com/document/d/1BUeSx-eziixhjZ9sOUydnKunwCkNZRdwGtNA6Ntl-18/edit#



`` Install Airflow, Hadoop, Spark``

> git clone https://gitlab.com/fusedataengineering/pipe_one/pipe_final_deepika.git


> Set environment variable for airflow :  export AIRFLOW_HOME=~/airflow


> Initiate airflow database: airflow db init


> Create user on airflow :

> airflow users create /
> 
> --username  /
> --firstname  /
> --lastname  /
> --role Admin /
> --email 
> start airflow web UI on daemon mode :
> -- airflow webserver -D
> start airflow scheduler on daemon mode :
> -- airflow scheduler -D


