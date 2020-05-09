import zipfile
from datetime import timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from sqlalchemy import create_engine
from airflow.operators.python_operator import PythonOperator

def insert_sql():

    zf = zipfile.ZipFile('/home/ec2-user/airflow_home/atp-tour-20002016.zip')
    df = pd.read_csv(zf.open('Data.csv'), encoding='ISO-8859-1')
    engine = create_engine('mysql+pymysql://admin:Yourpassword1@airflow.cu2iygn8rmbj.us-east-1.rds.amazonaws.com:3306/atp_tennis')
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
    dag_id='atp_dwnld_kaggle_ec2',
    default_args=default_args,
    description='ATP data from kaggle API',
    schedule_interval=timedelta(days=1),
)

t1 = BashOperator(
    task_id='run_kaggle_api',
    bash_command='kaggle datasets download -d jordangoblet/atp-tour-20002016 -p /home/ec2-user/airflow_home',
    dag=dag,
)

t2 = PythonOperator(
        task_id='move_data_from_file_to_SQL',
        provide_context=False,
        python_callable=insert_sql,
        dag=dag,
)

t1 >> t2