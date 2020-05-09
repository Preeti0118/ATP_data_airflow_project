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
from airflow.models import Variable

var_kaggle_api_cmd = Variable.get("var_kaggle_api_cmd")
var_path_unzip_from = Variable.get("var_path_unzip_from")
var_path_to = Variable.get("var_path_to")
var_file_name = Variable.get("var_file_name")
var_engine_path = Variable.get("var_engine_path")
var_csv_read_path = Variable.get("var_csv_read_path")
var_table = Variable.get("var_table")
var_input_notebook = Variable.get("var_input_notebook")
var_output_notebook = Variable.get("var_output_notebook")
df_atp = pd.DataFrame()

def unzip():
    zf = zipfile.ZipFile(var_path_unzip_from)
    df = pd.read_csv(zf.open(var_file_name),
                     dtype={"Winner": str, "Loser": str, "WRank": str, "LRank": str},
                     encoding='ISO-8859-1')
    # df.to_csv(var_path_to)
    global df_atp
    df_atp = df


def remove_columns():
    df_atp.drop(columns=[
                        'CBW', 'CBL', 'GBW', 'GBL', 'IWW', 'IWL', 'SBW', 'SBL', 'B365W', 'B365L',
                        'B&WW', 'B&WL', 'EXW', 'EXL', 'PSW', 'PSL', 'WPts', 'LPts', 'UBW', 'UBL',
                        'LBW', 'LBL', 'SJW', 'SJL', 'MaxW', 'MaxL', 'AvgW', 'AvgL'
                         ])


def save_csv():
    pass

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


def upload_raw_data():
    upload_file('/Users/psehgal/atp_test/Data.csv', 'preeti.first.boto.s3.bucket', 'atpdata')


def insert_sql():
    engine = create_engine(var_engine_path)
    df = pd.read_csv(var_csv_read_path, encoding='ISO-8859-1')
    df.to_sql(name=var_table, con=engine, index=False, if_exists='replace')


def call_jupyter():
    pm.execute_notebook(
                        var_input_notebook,
                        var_output_notebook,
                        parameters={'file_name': var_csv_read_path},
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
    dag_id='atp_dag_etl_process_flow_final',
    default_args=default_args,
    description='ATP data from kaggle API',
    schedule_interval=timedelta(days=1),
)

t1 = BashOperator(
    task_id='run_kaggle_api',
    bash_command=var_kaggle_api_cmd,
    dag=dag,
)

t2 = PythonOperator(
    task_id='unzip_api',
    provide_context=False,
    python_callable=unzip,
    dag=dag,
)

t3 = PythonOperator(
    task_id='remove_unwanted_columns',
    provide_context=False,
    python_callable=remove_columns,
    dag=dag,
)
# t3 = PythonOperator(
#         task_id='move_rawdata_to_S3',
#         provide_context=False,
#         python_callable=upload_raw_data,
#         dag=dag,
# )

t4 = PythonOperator(
        task_id='move_data_from_file_to_SQL',
        provide_context=False,
        python_callable=insert_sql,
        dag=dag,
)

# t5 = PythonOperator(
#         task_id='call_jupyter_from_pythionoperator',
#         provide_context=False,
#         python_callable=call_jupyter,
#         dag=dag,
# )

# t6 = PythonOperator(
#         task_id='move_pdfreport_to_s3',
#         provide_context=False,
#         python_callable=insert_pdfreport,
#         dag=dag,
# )

t1 >> t2 >> t4
# t1 >> t2 >> [t3, t4] >> t5 >> t6