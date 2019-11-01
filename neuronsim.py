import datetime
import json
import os
from datetime import timedelta
import pyodbc
import random


class GeoInfo:
    def __init__(self, geo_config: json):
        self.geo_config = geo_config
        self.remotes = []
        self.data_files = []
        # retrieve database info
        server = self.geo_config['odbc']['server']
        database = self.geo_config['odbc']['database']
        username = self.geo_config['odbc']['username']
        password = self.geo_config['odbc']['password']
        self.conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server}' +
                                   ';SERVER=' + server +
                                   ';DATABASE=' + database +
                                   ';UID=' + username +
                                   ';PWD=' + password)

    def __del__(self):
        self.conn.close()

    def __query_remotes(self):
        # count number of geo data files
        datafile_number = 0
        datafile_location = self.geo_config['datafile_location']
        __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
        geo_data_file_path = os.path.join(__location__, datafile_location)
        for file in os.listdir(geo_data_file_path):
            if file.startswith(self.geo_config['datafile_startswith']):
                datafile_number += 1
                # geo data file with absolute path
                file_name = os.path.join(__location__ + "/" + datafile_location, file)
                self.data_files.append(file_name)
        # retrieve remotes info from database
        cursor = self.conn.cursor()
        database = self.geo_config['odbc']['database']
        cursor.execute(f'SELECT TOP ({datafile_number}) [RemoteId] FROM [{database}].[dbo].[RemoteConfigCurrent]')
        rows = cursor.fetchall()
        # close cursor
        cursor.close()
        for row in rows:
            self.remotes.append(row[0])
        # self.remotes.sort()
        # self.data_files.sort()

    def parse_insert(self):
        remote_info = self.geo_config["remote"]
        cursor = self.conn.cursor()
        remote_index = 0
        self.__query_remotes()
        while remote_index < len(self.remotes):
            # remote info
            time_stamp_str = remote_info['start_date']
            time_stamp_start = datetime.datetime.strptime(time_stamp_str, '%Y-%m-%d')
            time_stamp_end = time_stamp_start + timedelta(days=remote_info['duration_day'])
            system_id = remote_info['system_id']
            remote_id = self.remotes[remote_index]
            geo_freq = remote_info['geo_freq_seconds']
            # all geo info
            geo_data = []
            # parse geo data file
            try:
                fp = open(self.data_files[remote_index], 'r', encoding='utf-8')
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
                    latitude_decimal = format(float(latitude_degree) + float(latitude_minutes) / 60 +
                                              float(latitude_seconds) / 3600, '.6f')
                    longitude = line_arr[1]
                    longitude_direction = longitude[0:1]
                    longitude_degree_index = longitude.index('°')
                    longitude_degree = longitude[1:longitude_degree_index]
                    longitude_minutes_index = longitude.index("'")
                    longitude_minutes = longitude[longitude_degree_index + 1:longitude_minutes_index]
                    longitude_seconds = longitude[longitude_minutes_index + 1:-1]
                    longitude_decimal = format(float(longitude_degree) + float(longitude_minutes) / 60 +
                                               float(longitude_seconds) / 3600, '.6f')
                    # save all geo data
                    geo_data_row = [latitude_decimal, longitude_decimal, latitude_direction, longitude_direction]
                    geo_data.append(geo_data_row)
            finally:
                fp.close()
            # insert into database for one remote
            time_stamp = time_stamp_start
            geo_data_index = 0
            # initial direction to travel geo data list is from beginning to ending
            geo_data_direction = 'f'
            while time_stamp < time_stamp_end:
                latitude_decimal = geo_data[geo_data_index][0]
                longitude_decimal = geo_data[geo_data_index][1]
                latitude_direction = geo_data[geo_data_index][2]
                longitude_direction = geo_data[geo_data_index][3]
                altitude = format(random.uniform(33000, 42000), '.6f')  # feet
                # debug info
                # print(f'{time_stamp}, {system_id}, {remote_id}: {latitude_decimal}{latitude_direction},'
                #       f'{longitude_decimal}{longitude_direction}, {altitude}')
                # insert one record into database
                cursor.execute("INSERT INTO [NEURONSim].[dbo].[RemoteLocation](\
                               [Timestamp], [SystemId], [RemoteId], [Latitude], [Longitude], [LatDirection],\
                               [LongDirection], [Altitude]) VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
                               time_stamp, system_id, remote_id, latitude_decimal, longitude_decimal,
                               latitude_direction, longitude_direction, altitude)
                self.conn.commit()
                # travel geo data list forwards
                if geo_data_direction == 'f':
                    geo_data_index += 1
                    # one position after the end of geo data list
                    if geo_data_index == len(geo_data):
                        # travel backwards
                        geo_data_direction = 'b'
                        # skip to the geo data before the last visited
                        geo_data_index -= 2
                # travel geo data list backwards
                elif geo_data_direction == 'b':
                    geo_data_index -= 1
                    # one position before the beginning of geo data list
                    if geo_data_index == -1:
                        # travel backwards
                        geo_data_direction = 'f'
                        # skip to the geo data after the last visited
                        geo_data_index += 2
                time_stamp += timedelta(seconds=geo_freq)
            remote_index += 1
        # close cursor
        cursor.close()
