from datetime import timedelta
import datetime
import pyodbc
import argparse


parser = argparse.ArgumentParser()
parser.add_argument("file_name", help="parse a geo location file", type=str)
args = parser.parse_args()

'''
/usr/bin/python3.6 /home/ronghua/PycharmProjects/python/parse_geo_file.py geo_file
'''
server = '192.168.88.164\ECMDB_MSRFT'
database = 'NEURONSim'
username = 'objown'
password = 'optimal123'
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+password)
cursor = conn.cursor()

timestamp_str = '2019-09-30 00:00:00'
timestamp = datetime.datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
systemid = 2
remoteid = 'remoteid_1234'
timespan = 6  # minute
altitude = '100.00'

try:
    fp = open(args.file_name, encoding='utf-8')
    line = fp.readline()
    cnt = 1
    while line:
        if cnt >= 3:
            line_arr = line.strip().split()

            latitude = line_arr[0]
            latitude_direction = latitude[0:1]
            latitude_degree_index = latitude.index('°')
            latitude_degree = latitude[1:latitude_degree_index]
            latitude_minutes_index = latitude.index("'")
            latitude_minutes = latitude[latitude_degree_index + 1:latitude_minutes_index]
            latitude_seconds = latitude[latitude_minutes_index + 1:-1]
            latitude_decimal = format(
                float(latitude_degree) + float(latitude_minutes) / 60 + float(latitude_seconds) / 3600, '.6f')

            longitude = line_arr[1]
            longitude_direction = longitude[0:1]
            longitude_degree_index = longitude.index('°')
            longitude_degree = longitude[1:longitude_degree_index]
            longitude_minutes_index = longitude.index("'")
            longitude_minutes = longitude[longitude_degree_index + 1:longitude_minutes_index]
            longitude_seconds = longitude[longitude_minutes_index + 1:-1]
            longitude_decimal = format(float(longitude_degree) + float(longitude_minutes) / 60 + float(longitude_seconds) / 3600, '.6f')
            print(f'{latitude_direction},{latitude_decimal} : {longitude_direction},{longitude_decimal}')
            #            cursor.execute("INSERT INTO [NEURONSim].[dbo].[RemoteLocation]
            #                           ([Timestamp], [SystemId], [RemoteId], [Latitude], [Longitude], [LatDirection], [LongDirection], [Altitude]) \
            #                           VALUES(?, ?, ?, ?, ?, ?, ?, ?)", \
            #                           timestamp, systemid, remoteid, latitude_decimal, longitude_decimal, latitude_direction, longitude_direction, altitude)
            #            conn.commit()
            timestamp += timedelta(minutes=6)
        line = fp.readline()
        cnt += 1
finally:
    fp.close()
