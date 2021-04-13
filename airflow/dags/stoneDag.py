from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
#from airflow.operators.python import PythonOperator
from functions.functions import *

args = {
    'owner': 'Airflow',
    'start_date': datetime(2021, 4, 11),
    'depends_on_past': True,
}

dag = DAG(
    dag_id='ETL_STONE_DAILY',
    schedule_interval=None, #'57 0 * * *', # programado para fazer o fluxo todo dia as 00:57
    default_args=args,
    description= 'ETL-STONE',
    tags=['Stone','Desafio','2021']
)

dummy_operator = DummyOperator(task_id='dummy_task', retries=3, dag=dag)

task_etl = PythonOperator(task_id='task_etl',
                         python_callable=stoneETL,
                         op_kwargs=None,
                         dag=dag
                         )

dummy_operator >> task_etl

