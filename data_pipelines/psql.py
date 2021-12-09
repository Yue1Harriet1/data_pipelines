from data_pipelines import env

import pyodbc
import pandas as pd
import os

CONNECTION_STRING = env.CONNECTION_STRING
CONNECTION_STRING = os.getenv(CONNECTION_STRING)

def pyodbc_query(query):
  cnxn = pyodbc.connect(CONNECTION_STRING)
  return(pd.read_sql(query, cnxn))

def df_to_db(df, db):
  cnxn = pyodbc.connect(CONNECTION_STRING)
  cursor = cnxn.cursor()
  columns = [c for c in df.columns.values]
  n = len(columns)
  values = ','.join(['?' for c in columns])
  column = ','.join(columns)
  sql = "INSERT INTO {0} ({1}) values({2})".format(db, columns, values)
  rows = ','.join(['row.iloc[{}]'.format(i) for i, c in enumerate(columns)])
  for index, row in df.iterrows():
    command = 'cursor.execute("{0}", {1})'.format(sql, rows)
    exec(command)
  cnxn.commit()
  cursor.close()
  
  
