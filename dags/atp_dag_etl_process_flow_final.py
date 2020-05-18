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


######################
#   Read Variables
######################

var_list = Variable.get("tennis_atp_variables", deserialize_json=True)

var_kaggle_api_cmd = var_list["var_kaggle_api_cmd"]
var_atp_report_cmd = var_list["var_atp_report_cmd"]
var_path_unzip_from = var_list["var_path_unzip_from"]
var_csv_path = var_list["var_csv_path"]
var_csv_path_clay = var_list["var_csv_path_clay"]
var_csv_path_grass = var_list["var_csv_path_grass"]
var_csv_path_hard = var_list["var_csv_path_hard"]
var_data_file_name = var_list["var_data_file_name"]
var_engine_path = var_list["var_engine_path"]
var_sql_table = var_list["var_sql_table"]
var_input_notebook_clay = var_list["var_input_notebook_clay"]
var_output_notebook_clay = var_list["var_output_notebook_clay"]
var_input_notebook_grass = var_list["var_input_notebook_grass"]
var_output_notebook_grass = var_list["var_output_notebook_grass"]
var_input_notebook_hard = var_list["var_input_notebook_hard"]
var_output_notebook_hard = var_list["var_output_notebook_hard"]
var_input_notebook_final = var_list["var_input_notebook_final"]
var_output_notebook_final = var_list["var_output_notebook_final"]
var_topclay1png = var_list['var_topclay1png']
var_topclay2png = var_list['var_topclay2png']
var_topclay3csv = var_list['var_topclay3csv']
var_topgrass1png = var_list['var_topgrass1png']
var_topgrass2png = var_list['var_topgrass2png']
var_topgrass3csv = var_list['var_topgrass3csv']
var_tophard1png = var_list['var_tophard1png']
var_tophard2png = var_list['var_tophard2png']
var_tophard3csv = var_list['var_tophard3csv']
var_atp_report = var_list['var_atp_report']
var_s3_bucket = var_list['var_s3_bucket']

#######################
#   Functions
#######################


def unzip():
    zf = zipfile.ZipFile(var_path_unzip_from)
    df = pd.read_csv(zf.open(var_data_file_name),
                     dtype={"Winner": str, "Loser": str, "WRank": str, "LRank": str},
                     encoding='ISO-8859-1')
    df.to_csv(var_csv_path)


def remove_columns():
    df = pd.read_csv(var_csv_path, encoding='ISO-8859-1')
    df.drop(columns=[
        'CBW', 'CBL', 'GBW', 'GBL', 'IWW', 'IWL', 'SBW', 'SBL', 'B365W', 'B365L',
        'B&WW', 'B&WL', 'EXW', 'EXL', 'PSW', 'PSL', 'WPts', 'LPts', 'UBW', 'UBL',
        'LBW', 'LBL', 'SJW', 'SJL', 'MaxW', 'MaxL', 'AvgW', 'AvgL',
    ], axis=1, inplace=True)
    df.to_csv(var_csv_path)
    df_clay = df.loc[df['Surface'] == 'Clay']
    df_grass= df.loc[df['Surface'] == 'Grass']
    df_hard = df.loc[df['Surface'] == 'Hard']
    df_clay.to_csv(var_csv_path_clay)
    df_grass.to_csv(var_csv_path_grass)
    df_hard.to_csv(var_csv_path_hard)

def upload_raw_data():
    upload_file(var_csv_path, var_s3_bucket, 'atpdata.csv')

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


def insert_sql():
    engine = create_engine(var_engine_path)
    df = pd.read_csv(var_csv_path, encoding='ISO-8859-1')
    df.to_sql(name=var_sql_table, con=engine, index=False, if_exists='replace')


def call_jupyter_clay():
    pm.execute_notebook(
        var_input_notebook_clay,
        var_output_notebook_clay,
        parameters={
                    'file_name': var_csv_path_clay,
                    'topclay1png': var_topclay1png,
                    'topclay2png': var_topclay2png,
                    'topclay3csv': var_topclay3csv
                    }

    )

def call_jupyter_grass():
    pm.execute_notebook(
        var_input_notebook_grass,
        var_output_notebook_grass,
        parameters={
                    'file_name': var_csv_path_grass,
                    'topgrass1png': var_topgrass1png,
                    'topgrass2png': var_topgrass2png,
                    'topgrass3csv': var_topgrass3csv
                    }

    )
def call_jupyter_hard():
    pm.execute_notebook(
        var_input_notebook_hard,
        var_output_notebook_hard,
        parameters={
                    'file_name': var_csv_path_hard,
                    'tophard1png': var_tophard1png,
                    'tophard2png': var_tophard2png,
                    'tophard3csv': var_tophard3csv
                    }

    )

def call_jupyter_final():
    pm.execute_notebook(
        var_input_notebook_final,
        var_output_notebook_final,
        parameters={
                    'var_topclay1png': var_topclay1png,
                    'var_topclay2png': var_topclay2png,
                    'var_topclay3csv': var_topclay3csv,
                    'var_topgrass1png': var_topgrass1png,
                    'var_topgrass2png': var_topgrass2png,
                    'var_topgrass3csv': var_topgrass3csv,
                    'var_tophard1png': var_tophard1png,
                    'var_tophard2png': var_tophard2png,
                    'var_tophard3csv': var_tophard3csv
                    }

    )
def upload_atpreport():
    upload_file(var_atp_report, var_s3_bucket, 'atp_report.html')


#########################
#  Dags Pipeline code
#########################


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
    task_id='get_data_using_api',
    bash_command=var_kaggle_api_cmd,
    dag=dag,
)

t2a = PythonOperator(
     task_id='unzip_api_data',
     provide_context=False,
     python_callable=unzip,
     dag=dag,
)

t2b = PythonOperator(
     task_id='cleanse_data',
     provide_context=False,
     python_callable=remove_columns,
     dag=dag,
)

t3 = PythonOperator(
        task_id='upload_cleansed_data_to_S3',
        provide_context=False,
        python_callable=upload_raw_data,
        dag=dag,
)

t4 = PythonOperator(
     task_id='insert_cleansed_data_into_SQL',
     provide_context=False,
     python_callable=insert_sql,
     dag=dag,
)

t5 = PythonOperator(
        task_id='top_clay_players',
        provide_context=False,
        python_callable=call_jupyter_clay,
        dag=dag,
)

t6 = PythonOperator(
        task_id='top_grass_players',
        provide_context=False,
        python_callable=call_jupyter_grass,
        dag=dag,
)

t7 = PythonOperator(
        task_id='top_hard_players',
        provide_context=False,
        python_callable=call_jupyter_hard,
        dag=dag,
)

t8a = PythonOperator(
        task_id='final_report',
        provide_context=False,
        python_callable=call_jupyter_final,
        dag=dag,
)

t8b = BashOperator(
    task_id='convert_report_to_html',
    bash_command=var_atp_report_cmd,
    dag=dag,
)

t9 = PythonOperator(
        task_id='upload_html_report_to_s3',
        provide_context=False,
        python_callable=upload_atpreport,
        dag=dag,
)

t1 >> t2a >> t2b >> [t3, t4, t5, t6, t7] >> t8a >> t8b >> t9

