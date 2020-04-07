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

def insert_sql():
    df = pd.read_csv('/Users/psehgal/Data.csv', encoding='ISO-8859-1')
    engine = create_engine('mysql+pymysql://root:yourpassword@localhost:3306/ATP_tennis')
    df.to_sql(name='atprawdata', con=engine, index=False, if_exists='replace')


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
    dag_id='atp_data03',
    default_args=default_args,
    description='ATP data from kaggle API',
    schedule_interval=timedelta(days=1),
)

t1 = BashOperator(
    task_id='run_kaggle_api',
    bash_command='kaggle datasets download -d jordangoblet/atp-tour-20002016 -p /Users/psehgal/atp_test/',
    dag=dag,
)

t2 = PythonOperator(
        task_id='move_data_from_file_to_SQL',
        provide_context=False,
        python_callable=insert_sql,
        dag=dag,
)

t1 >> t2
#