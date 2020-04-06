import pandas as pd
import sqlalchemy
import zipfile
from zipfile import ZipFile
import pymysql
import numpy as np

df = pd.read_csv('/Users/psehgal/Data.csv', encoding = 'ISO-8859-1')
print('shape of dataframe = ', df.shape)
df.drop(['CBW','CBL','GBW','GBL','IWW','IWL'],axis=1,inplace=True)
df.drop(['SBW','SBL'],axis=1,inplace=True)
df.drop(['B365W','B365L','B&WW','B&WL','EXW','EXL','PSW','PSL'],axis=1,inplace=True)
df.drop(['WPts','LPts','UBW','UBL','LBW','LBL','SJW'],axis=1,inplace=True)
df.drop(['SJL','MaxW','MaxL','AvgW','AvgL'],axis=1,inplace=True)
print('shape of dataframe = ', df.shape)
df['indexkey'] = (range(1, len(df) + 1))
df.set_index('indexkey', inplace=True)
print('columns of dataframe = ', df.columns)
print(df.describe())
print(df.loc[1])
engine = sqlalchemy.create_engine('mysql+pymysql://root:yourpassword@localhost:3306/ATP_tennis')
print('connection successful')
df.to_sql(name='atprawdata', con=engine, index=False, if_exists='replace')
#df.to_sql(name='atprawdata1', con=engine, schema='ATP_tennis')
print('all steps complete')
