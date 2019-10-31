import argparse
import datetime
import json
import os
from datetime import timedelta
import pyodbc
import random

parser = argparse.ArgumentParser()
parser.add_argument("config_file", help="configuration file of geo location data", type=str)
args = parser.parse_args()

__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
config_file = os.path.join(__location__, args.config_file)

# parse configuration file
with open(config_file, "r") as config_data:
    all_info = json.load(config_data)

# sql server info
server = all_info['odbc']['server']
database = all_info['odbc']['database']
username = all_info['odbc']['username']
password = all_info['odbc']['password']

conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database +
                      ';UID=' + username + ';PWD=' + password)
cursor = conn.cursor()

# datafile location
datafile_location = all_info['datafile_location']

remotes = all_info['remotes']
for remote in remotes:
    # geo data file
    file_name = os.path.join(__location__ + "/" + datafile_location, remote['geo_data_name'])
    # remote info
    time_stamp_str = remote['start_date']
    time_stamp_start = datetime.datetime.strptime(time_stamp_str, '%Y-%m-%d')
    time_stamp_end = time_stamp_start + timedelta(days=remote['duration_day'])
    system_id = remote['system_id']
    remote_id = remote['remote_id']
    geo_freq = remote['geo_freq_seconds']
    # all geo info
    geo_data = []
    # parse geo data file
    try:
        fp = open(file_name, 'r', encoding='utf-8')
        for line in fp:
            if not line.strip() or line.startswith('#'):
                continue

            line_arr = line.strip().split()

            latitude = line_arr[0]
            latitude_direction = latitude[0:1]
            latitude_degree_index = latitude.index('°')
            latitude_degree = latitude[1:latitude_degree_index]
            latitude_minutes_index = latitude.index("'")
            latitude_minutes = latitude[latitude_degree_index + 1:latitude_minutes_index]
            latitude_seconds = latitude[latitude_minutes_index + 1:-1]
            latitude_decimal = format(float(latitude_degree) + float(latitude_minutes) / 60 + float(latitude_seconds) / 3600, '.6f')

            longitude = line_arr[1]
            longitude_direction = longitude[0:1]
            longitude_degree_index = longitude.index('°')
            longitude_degree = longitude[1:longitude_degree_index]
            longitude_minutes_index = longitude.index("'")
            longitude_minutes = longitude[longitude_degree_index + 1:longitude_minutes_index]
            longitude_seconds = longitude[longitude_minutes_index + 1:-1]
            longitude_decimal = format(float(longitude_degree) + float(longitude_minutes) / 60 + float(longitude_seconds) / 3600, '.6f')

            # save all geo data
            geo_data_row = [latitude_decimal, longitude_decimal, latitude_direction, longitude_direction]
            geo_data.append(geo_data_row)
    finally:
        fp.close()

    # insert into database for one remote
    # print(len(geo_data))
    time_stamp = time_stamp_start
    geo_data_index = 0
    geo_data_direction = 'f'
    while time_stamp < time_stamp_end:
        latitude_decimal = geo_data[geo_data_index][0]
        longitude_decimal = geo_data[geo_data_index][1]
        latitude_direction = geo_data[geo_data_index][2]
        longitude_direction = geo_data[geo_data_index][3]
        altitude = random.uniform(33000, 42000)  # feet
        # print(f'{time_stamp}, {system_id}, {remote_id}: {latitude_decimal}{latitude_direction}, {longitude_decimal}{longitude_direction}')
        # insert one record into database
        cursor.execute("INSERT INTO [NEURONSim].[dbo].[RemoteLocation]([Timestamp], [SystemId], [RemoteId], [Latitude],\
                        [Longitude], [LatDirection], [LongDirection], [Altitude]) VALUES(?, ?, ?, ?, ?, ?, ?, ?)",\
                        time_stamp, system_id, remote_id, latitude_decimal, longitude_decimal, latitude_direction,\
                        longitude_direction, altitude)
        conn.commit()

        if geo_data_direction == 'f':
            geo_data_index += 1
            if geo_data_index == len(geo_data):
                geo_data_direction = 'b'
                geo_data_index -= 2
        elif geo_data_direction == 'b':
            geo_data_index -= 1
            if geo_data_index == -1:
                geo_data_direction = 'f'
                geo_data_index += 2

        time_stamp += timedelta(seconds=geo_freq)
