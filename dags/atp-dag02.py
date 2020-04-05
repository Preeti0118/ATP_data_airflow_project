from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
#from airflow.utils.dates import datetime
import boto3
import logging
from botocore.exceptions import ClientError
import os
# import pandas as pd
#import zipfile
from zipfile import ZipFile
# import pandas as pd
# import sqlalchemy
# from sqlalchemy import Table, Column, Integer, String, MetaData



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
    with ZipFile('/Users/psehgal/atp-tour-20002016.zip', 'r') as zipObj:
        #    Extract all the contents of zip file in different directory
        zipObj.extractall('/Users/psehgal')

    upload_file('/Users/psehgal/Data.csv', 'preeti.first.boto.s3.bucket', 'atpdata')


def delete_files():
    os.remove('/Users/psehgal/Data.csv')
    os.remove('/Users/psehgal/atp-tour-20002016.zip')


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
    dag_id='atp_data02',
    default_args=default_args,
    description='ATP data from kaggle API',
    schedule_interval=timedelta(days=1),
)

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id='run_kaggle_api',
    bash_command='kaggle datasets download -d jordangoblet/atp-tour-20002016',
    dag=dag,
)


t2 = PythonOperator(
        task_id='move_file_to_AWS_S3',
        #'start_date': days_ago(2),
        provide_context=True,
        python_callable=insert_data,
        dag=dag
)

t3 = PythonOperator(
        task_id='remove_files',
        #'start_date': days_ago(2),
        provide_context=True,
        python_callable=delete_files,
        dag=dag

)


t1 >> t2 >> t3
