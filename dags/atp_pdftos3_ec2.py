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

def upload_file(file_name, bucket, object_name=None):

    if object_name is None:
        object_name = file_name

    s3_client = boto3.client('s3')

    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def insert_pdfreport():
    upload_file('/home/ec2-user/airflow_home/atp_test/atp_mens_tour_pdf_report.pdf', 'preeti.first.boto.s3.bucket',
                'pdfreportfromec2')


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
    dag_id='atp_pdfec2_to_s3',
    default_args=default_args,
    description='ATP data from kaggle API',
    schedule_interval=timedelta(days=1),
)


tt2 = PythonOperator(
        task_id='move_file_to_AWS_S3',
        provide_context=False,
        python_callable=insert_pdfreport,
        dag=dag
)