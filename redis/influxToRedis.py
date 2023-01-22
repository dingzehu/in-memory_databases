import redis
import pandas as pd
import pickle
import datetime
import influxdb
import time
import sys
import numpy as np
import statistics
from datetime import timedelta, datetime, date
from influxdb import InfluxDBClient, DataFrameClient

p = inflect.engine()

HOST = os.getenv("HOST")
PORT = '8086'
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
db = os.getenv("DB")

client = InfluDBClient(HOST, PORT, USER, PASSWORD, db, ssl=True, verify_ssl=True)
r = redis.Redis(host='localhost', port=6379, db=0)
    #decode_responses=True

start_time = str('2017-01-01')
format = "%Y-%m-%d"
start_date = date(2017, 1, 1)
end_date = date(2017, 1, 2)
#end_date = date.today()-timedelta(1)
#end_date = str(date.today()-timedelta(1))
#query_times = time(input())

delta_data = (end_date-start_date).days

#date_params = {'start_time': start_time, 'end_time': end_time}
data_params = {'start_time': start_time}

influx_list = []
redis_insert_list = []
redis_return_list = []

#print(str(start_date+timedelta(1)))
#for n in range(1, query_times+1):
results = []
#results_later = []
i = 1
while i < delta_date+1:
    if i ==1:
        start = str(start_date)
        end = str(end_date)
        #end = str(start_date+timedelta(1))
        input_params = {'start': start, 'end': end}
        influx_query_start_time = time.perf_counter()
        result = client.query('SELECT * FROM sensor WHERE time > $start AND time < $end', bind_params=input_params)
        influx_query_end_time = time.perf_counter()
        points = result.get_points()
        for point in points:
            results.append(point)
        start_mod = end
        i += 1



        df_use = pd.DataFrame(results, columns=['time', 'P1', 'P2', 'durP1', 'durP2', 'humidity', 'lat', 'location', 'lon', 'ratioP1', 'ratioP2', 'sensor_id', 'sensor_type', 'temperature'])

        df_use['time'] = pd.to_datetime(df_use['time'])

        df_use_dict = df_use.to_dict('records')

        '''
        #using redis.zadd
        params_dict = {}
        for d in df_use_dict:
            params_dict.setdefault(pickle.dumps(d), d.get('time').timestamp())
        redis_insert_start_time = time.perf_counter()
        r.zadd('sensor', mapping=params_dict)
        redis_insert_end_time = time.perf_counter()
        '''

        #using pipeline
        pipe = r.pipeline()
        redis_insert_start_time = time.perf_counter()
        for d in df_use_dict:
            pipe.zadd(name='sensor', mapping={pickle.dumps(d): d.get('time').timestamp()})
        redis_insert_end_time = time.perf_counter()
        pipe.execute()


        #elasped_redis_insert = redis_insert_end_time - redis_start_time
        #redis_insert_list.append(elasped_redis_insert)
        #redis_insert_array = np.array(redis_insert_list)

        redis_read_start_time = time.perf_counter()
        final_result = r.zrange('sensor', 0, -1)
        #for i in final_result:
            #print(pickle.loads(i))
        redis_read_end_time = time.perf_counter()

        #elasped_redis_return = redis_read_end_time - redis_read_start_time
        #redis_return_list.append(elasped-redis_return)
        #redis_return_array = np.array(redis_return_list)
#for b in final_result:
    #print(pickle.loads(b))

        print("InfluxDB query: %s seconds" % (influx_query_end_time - influx_query_start_time))
        print("Insert to redis: %s seconds "% (redis_insert_end_time - redis_insert_start_time))
        print("Return from redis %s seconds" % (redis_read_end_time - redis_read_start_time))
        print("Number of records: %s" % len(results))




        continue
    elif i > 1:
        results_later = []
        end = str(datetime.strptime(start_mod, '%Y-%m-%d').date()+timedelta(1))
        print(start_mod)
        print(end)
        input_params_later = {'start': start_mod, 'end': end}
        print("query start")
        influx_query_start_time = time.perf_counter()
        result_later = client.query('SELECT * FROM sensor WHERE time > $start AND time<= $end', bind_params = input_params_later)
        influx_query_end_time = time.perf_counter()
        print("query end")
        points_later = result_later.get_points()
        for point_later in points_later:
            results_later.append(point_later)
        start_mod = end
        i += 1

        df_use_later = pd.DataFrame(results_later, columns=['time', 'P1', 'P2', 'durP1', 'durP2', 'humidity', 'lat', 'location', 'lon', 'ratioP1', 'ratioP2', 'sensor_id', 'sensor_type', 'temperature'], dtype=object)

        df_use_later['time'] = pd.to_datetime(df_use_later['time'])

        df_use_dict_later = df_use_later.to_dict('records')

        '''
        #using redis.zadd
        params_dict = {}
        for d_later in df_use_dict_later:
            params_dict.setdefault(pickle.dumps(d_later), d_later.get('time').timestamp())
        redis_insert_start_time_later = time.perf_counter()
        r.zadd('sensor', mapping=params_dict)
        redis_insert_end_time_later = time.perf_counter()
        '''

        #using pipeline
        pipe = r.pipeline()
        redis_insert_start_time_later = time.perf_counter()
        for d_later in df_use_dict_later:
            pipe.zadd(name='sensor', mapping{pickle.dumps(d_later): d_later.get('time')timestamp()})
        redis_insert_end_time_later = time.perf_counter()
        pipe.execute()

        
        #elasped_redis_insert_later = redis_insert_end_time_later - redis_insert_start_time_later
        #redis_insert_list.append(elasped_redis_insert_later)
        #redis_insert_array = np.array(redis_insert_list)

        redis_read_start_time_later = time.perf_counter()
        final_result_later = r.zrange('sensor', 0, -1)
        for i_later in final_result_later:
            print(pickle.loads(i_later))
        redis_read_end_time_later = time.perf_counter()


        #elasped_redis_return_later = redis_read_end_time_later - redis_read_start_time_later
        #redis_return_list.append(elasped_redis_return)
        #redis_return_array = np.array(redis_return_list)
#for b in final_result:
    #print(pickle.load(b))

#print("InfluxDB query: %d seconds" % (influx_query_end_time - influx_query_start_time))
        print("Insert to redis: %s seconds " % (redis_insert_end_time_later - redis_insert_start_time_later))
        print("Return from redis: %s seconds" % (redis_read_end_time_later - redis_read_start_time_later))
        print("Number of records: %s" %len(results_later))



        continue

print(r.zcount("sensor", 0, -1))
print("Insert finished")

#results.extend(results_later)
#print(results)

#print(len(results))
#for n in range(1, delta_date+1+:

