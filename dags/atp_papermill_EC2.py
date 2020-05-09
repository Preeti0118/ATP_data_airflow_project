from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.papermill_operator import PapermillOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import boto3
import logging
from botocore.exceptions import ClientError
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import logging
from botocore.exceptions import ClientError
import os
import zipfile
from zipfile import ZipFile
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData
import pymysql
import papermill as pm

# ## Syntax for python operator
# import papermill as pm
#
# pm.execute_notebook('weather_forecast_using_pyowm.ipynb',
#                     'weather_forecast_using_pyowm_output.ipynb',
#                     parameters={'city':'Sao Paulo,BR'},
#### End of Syntax

def call_jupyter():
    pm.execute_notebook('/home/ec2-user/airflow_home/atp_test/atp_mens_tour.ipynb',
                        '/home/ec2-user/airflow_home/atp_test/atp_mens_tour_output.ipynb',
                        parameters={'file_name': '/home/ec2-user/airflow_home/atp_test/Data.csv'},

                        )
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['preetisehgal2001@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),

}

dag = DAG(
    dag_id='atp_papermill_EC2',
    default_args=default_args,
    description='ATP data from kaggle API',
    schedule_interval=timedelta(days=1),
)

t6 = PythonOperator(
        task_id='call_jupyter_from_pythionoperator',
        provide_context=False,
        python_callable=call_jupyter,
        dag=dag,
)
