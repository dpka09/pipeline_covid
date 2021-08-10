import airflow
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from transformation import *
from main import *


def fuse_task1():
    load_df(task1(), 1)
    
def fuse_task2():
    load_df(task2(), 2)

def fuse_task3():
    load_df(task3(), 3)

def fuse_task4():
    load_df(task4(), 4)

def fuse_task5():
    load_df(task5(), 5)



default_args={

            'owner':'postgres',
            'start_date': airflow.utils.dates.days_ago(1),
            'depends_on_past': True,
            'email':['sthadpka93@gmail.com'],
            'email_on_failure':True,
            'email_on_retry': False,
            'retries': 7,
            'retry_delay': timedelta(minutes=3)
}

dag = DAG (dag_id= "Covid",
            default_args= default_args,
            schedule_interval= "0 0 0 0 0" )

task_1= PythonOperator(task_id="Covidtask1",
                      python_callable=fuse_task1,
                      dag =dag       
                     )


task_2= PythonOperator(task_id="Covidtask2",
                      python_callable=fuse_task2,
                      dag =dag       
                     )

task_3= PythonOperator(task_id="Covidtask3",
                      python_callable=fuse_task3,
                      dag =dag       
                     )

task_4= PythonOperator(task_id="Covidtask4",
                      python_callable=fuse_task4,
                      dag =dag       
                     )

task_5= PythonOperator(task_id="Covidtask5",
                      python_callable=fuse_task5,
                      dag =dag       
                     )


task_1 >>   task_2 >>   task_3 >>   task_4 >>   task_5 