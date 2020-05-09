import zipfile
from datetime import timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from sqlalchemy import create_engine
from airflow.operators.python_operator import PythonOperator









# zf = zipfile.ZipFile('/home/ec2-user/airflow_home/atp-tour-20002016.zip')
# df = pd.read_csv(zf.open('Data.csv'), encoding='ISO-8859-1')
# engine = create_engine('mysql+pymysql://admin:Yourpassword1@airflow.cu2iygn8rmbj.us-east-1.rds.amazonaws.com:3306/atp_tennis')
# df.to_sql(name='atprawdata', con=engine, index=False, if_exists='replace')
