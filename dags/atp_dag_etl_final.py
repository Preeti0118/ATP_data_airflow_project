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



def unzip():
    zf = zipfile.ZipFile('/Users/psehgal/atp_test/atp-tour-20002016.zip')
    df = pd.read_csv(zf.open('Data.csv'), encoding='ISO-8859-1')
    df.to_csv('/Users/psehgal/atp_test/Data.csv')

def upload_file(file_name, bucket, object_name=None):
    """
    upload file to an S3 bucket
    :param file_name: file to be upkoaded
    :param bucket:
    :param object_name:
    :return:
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')

    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def insert_data():
    upload_file('/Users/psehgal/atp_test/Data.csv', 'preeti.first.boto.s3.bucket', 'atpdata')



def insert_sql():
    engine = create_engine('mysql+pymysql://root:yourpassword@localhost:3306/ATP_tennis')
    df = pd.read_csv('/Users/psehgal/atp_test/Data.csv', encoding='ISO-8859-1')
    df.to_sql(name='atprawdata', con=engine, index=False, if_exists='replace')

def call_jupyter():
    pm.execute_notebook('/Users/psehgal/dev/airflow_home/atp_mens_tour.ipynb',
                        '/Users/psehgal/dev/airflow_home/atp_mens_tour_output.ipynb',
                        parameters={'file_name': '/Users/psehgal/atp_test/Data.csv'},

                        )

def insert_pdfreport():
    upload_file('/Users/psehgal/dev/airflow_home/atp_mens_tour_pdf_report.pdf', 'preeti.first.boto.s3.bucket',
                'pdfreport')


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
    dag_id='atp_dag_etl_final',
    default_args=default_args,
    description='ATP data from kaggle API',
    schedule_interval=timedelta(days=1),
)

t1 = BashOperator(
    task_id='run_kaggle_api',
    bash_command='kaggle datasets download -d jordangoblet/atp-tour-20002016 -p /Users/psehgal/atp_test',
    dag=dag,
)

t2 = PythonOperator(
    task_id='unzip_api',
    provide_context=False,
    python_callable=unzip,
    dag=dag,
)

t3 = PythonOperator(
        task_id='move_file_to_AWS_S3',
        provide_context=False,
        python_callable=insert_data,
        dag=dag,
)

t4 = PythonOperator(
        task_id='move_data_from_file_to_SQL',
        provide_context=False,
        python_callable=insert_sql,
        dag=dag,
)

t5 = PythonOperator(
        task_id='call_jupyter_from_pythionoperator',
        provide_context=False,
        python_callable=call_jupyter,
        dag=dag,
)

t6 = PythonOperator(
        task_id='move_pdfreport_to_s3',
        provide_context=False,
        python_callable=insert_pdfreport,
        dag=dag,
)


t1 >> t2 >> [t3, t4] >> t5 >> t6