from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
#from airflow.operators.python import PythonOperator
from functions.functions import *
from airflow.utils.dates import days_ago
import requests

args = {
    'owner': 'Airflow',
#    'start_date': days_ago(2)
    'start_date': datetime(2021, 4, 11)
}

dag = DAG(
    dag_id='ETL_IBGE',
    schedule_interval=None, # programado para não se repetir
    default_args=args,
    description= 'ETL-IBGE',
    tags=['Stone', 'Desafio','IBGE']
)

dummy_operator = DummyOperator(task_id='dummy_task', retries=3, dag=dag)

task_etl = PythonOperator(task_id='task_etl',
                         python_callable=ibgeETL,
                         op_kwargs=None,
                         dag=dag
                         )
                         

dummy_operator >> task_etl

