from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import zipfile
import pandas as pd
import sqlalchemy
from sqlalchemy import Table, Column, Integer, String, MetaData


def insert_data():

    zf = zipfile.ZipFile('/Users/psehgal/atp-tour-20002016.zip')
    df = pd.read_csv(zf.open('atp-tour-20002016.csv'))
    engine = sqlalchemy.create_engine('mysql+pymysql://root:yourpassword@localhost/ATP_tennis')
    df.to_sql(name='simple_result', con=engine, index=False, if_exists='append')



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
    dag_id = 'atp_data01',
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
        task_id='insert_in_sql',
        provide_context = True,
        python_callable = insert_data,
        dag=dag
)


t1 >> t2

#dag.doc_md = __doc__

#t1.doc_md = """\
#### Task Documentation
#You can document your task using the attributes `doc_md` (markdown),
#`doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
#rendered in the UI's Task Instance Details page.
#![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
#"""
#templated_command = """
#{% for i in range(5) %}
#    echo "{{ ds }}"
#    echo "{{ macros.ds_add(ds, 7)}}"
#    echo "{{ params.my_param }}"
#{% endfor %}
#"""

#t3 = BashOperator(
#    task_id='templated',
#    depends_on_past=False,
#    bash_command=templated_command,
#    params={'my_param': 'Parameter I passed in'},
#    dag=dag,
##)
#
#t1 >> [t2, t3]

