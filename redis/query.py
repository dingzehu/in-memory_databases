import pandas as pd
import datetime
import influxdb
import time
import sys
import numpy as np
import statistics
import inflect
import datetime as dt
from datetime import timedelta, datetime, date
from influxdb import InfluxDBClient, DataFrameClient
from sklearn.preprocessing import LabelEncoder

labelencoder = LabelEncoder()

p = inflect.engine()

HOST = os.getenv("HOST")
PORT = '8086'
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
db = os.getenv("DB")

client = InfluxDBClient(HOST, PORT, USER, PASSWORD, db, ssl=True, verify_ssl=True)

start_time = str(input("start date: "))
end_time = str(input("end date: "))

date_params = {'start_time': start_time, 'end_time': end_time}

results = []

result = client.query('SELECT * FROM sensor WHERE time > $start_time AND time < $end_time', bind_params=date_params)
points = result.get_points()
for point in points:
    results.append(point)

df_use = pd.DataFrame(results, columns=['time', 'P1', 'P2', 'durP1', 'durP2', 'humidity',
    'lat', 'location', 'lon', 'ratioP1', 'ratioP2', 'sensor_id', 'sensor_type', 'temperature'])

#df_use['sensor_type'] = labelencoder.fit_transform(df_use['sensor_type'])

df_use['time'] = pd.to_datetime(df_use['time'])

#df_use['sensor_type'] = df_use['sensor_type'].astype('int8')

print(df_use)

print(df_use.info(memory_usage='deep'))

print(df_use.memory_usage(deep=True))

print(df_use.memory_usage(deep=True).sum())
