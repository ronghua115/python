import pyodbc

# conn = pyodbc.connect('Driver={SQL Server};'
#                      'Server=192.168.88.164\ECMDB_MSRFT;'
#                      'Database=NEURONSim;'
#                      'Trusted_Connection=yes;')
'''
windows call
conn = pyodbc.connect('DRIVER={SQL Server};'
                      'SERVER=192.168.88.164\ECMDB_MSRFT;'
                      'PORT=1433;'
                      'DATABASE=NEURONSim;'
                      'UID=objown;'
                      'PWD=optimal123')
'''
# ubuntu call
server = '192.168.88.164\ECMDB_MSRFT'
database = 'NEURONSim'
username = 'objown'
password = 'optimal123'
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)

cursor = conn.cursor()
cursor.execute('SELECT TOP(1)*\
                FROM dbo.RemoteState\
                WHERE Timestamp > \'2019-10-05 23:59:20\'')

for row in cursor:
    print(row)

print('==================================================')

cursor.execute('''
                SELECT TOP(10)*
                FROM dbo.RemoteState
                WHERE Timestamp > '2019-10-05 23:59:20'
                ''')
for row in cursor:
    print(row)
