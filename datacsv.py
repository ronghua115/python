import pandas as pd
# import numpy as np
import time
from sqlalchemy import create_engine, event
from urllib.parse import quote_plus

conn = "DRIVER={ODBC Driver 17 for SQL Server};" + \
       "SERVER=192.168.88.164\\ECMDB_MSRFT;" + \
       "DATABASE=NEURONSim;" + \
       "UID=objown;" + \
       "PWD=optimal123"
quoted = quote_plus(conn)
new_con = 'mssql+pyodbc:///?odbc_connect={}'.format(quoted)
engine = create_engine(new_con, fast_executemany=True)

'''
CREATE TABLE [NEURONSim].[dbo].[fast_executemany_test](
    [id] [uniqueidentifier] DEFAULT NEWID() NOT NULL,
    [time] [datetime2](0) NOT NULL,
    [name] [varchar](25) NOT NULL,
    [age] [int] NOT NULL,
    [score] [float] NOT NULL)
'''
table_name = 'fast_executemany_test'
# df = pd.DataFrame(np.random.random((10 ** 4, 100)))
df = pd.read_csv("data.csv",
                 names=['time', 'name', 'age', 'score'],
                 parse_dates=['time'],
                 dtype={'name': str, 'age': int, 'score': float},
                 index_col=False)

s = time.time()
df.to_sql(table_name, engine, if_exists='append', chunksize=None, index=False)
print('execution time: {}'.format(time.time() - s))
