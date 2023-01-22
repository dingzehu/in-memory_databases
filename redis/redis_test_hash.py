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

#p = inflect.engine()

HOST = os.getenv("HOST")
PORT = '8086'
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
db = os.getenv("DB")

client = InfluxDBClient(HOST, PORT, USER, PASSWORD, db, ssl=True, verify_ssl=True)
r = redis.Redis(host='localhost', port=6379, db=0, password='mypassword')

start_time = str('2017-01-01')
format = "%Y-%m-%d"
start_date = date(2017, 1, 1)
end_date = date(2017, 1, 5)
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
#results = []
#results_later = []
#i = 1
#while i < delta_date+1:
#    if i ==1:
start = str(start_date)
end = str(end_date)

query = []
insert = []
read = []

        #end = str(start_date+timedelta(1))
for i in range(5):
    results = []
    input_params = {'start': start, 'end': end}
    influx_query_start_time = time.perf_counter()
    result = client.query('SELECT * FROM sensor WHERE time >= $start AND time < $end', bind_params=input_params)
    influx_query_end_time = time.perf_counter()
    points = result.get_points()
    for point in points:
        results.append(point)
#start_mod = end
#i += 1

    print("query from influxdb done")

    df_use = pd.DataFrame(results, columns=['time', 'P1', 'P2', 'durP1', 'durP2', 'humidity', 'lat', 'location', 'lon', 'ratioP1', 'ratioP2', 'sensor_id', 'sensor_type', 'temperature'])

    df_use['time'] = pd.to_datetime(df_use['time'])

    df_use_datetime = df_use.copy()

    df_use['time'] = df_use['time'].astype(str)

    #df_use_dict = df_use.to_dict('records')
    
    df_use_dict = df_use.values.tolist()

    #print(df_use.dtypes)
    #print(df_use_dict) 

    '''
    #using redis.zadd
    params_dict = {}
    for d in df_use_dict:
        params_dict.setdefault(pickle.dumps(d), d.get('time').timestamp())
    redis_insert_start_time = time.perf_counter()
    r.zadd('sensor', mapping=params_dict)
    redis_insert_end_time = time.perf_counter()
    '''
    #pieces = []
    #using pipeline
    pipe = r.pipeline()
    redis_insert_start_time = time.perf_counter()
    for count, d, row in zip(range(len(results)), df_use_dict, df_use.index):
    #for pairs in df_use_dict.iteritems():
        #pieces.append(pair[1])
        #pieces.append(pair[0])
        #r.hmset("sensor", d)
        #print(d)
        #pipe.zadd(name="sensor", mapping={pickle.dumps(d): d.get("time").timestamp()})
        #print(d.get("time"))
        
        #print(df_use_datetime['time'][row])
        #print(df_use_datetime['time'][row].timestamp())
        #pipe.hmset((df_use['time'].iloc[count]), d)

        #mapping = {member: score}
        #pipe.zadd("openforecast", mapping={(df_use['time'].iloc[count]): df_use_datetime['time'][row].timestamp()})
        pipe.zadd("openforecast", mapping={pickle.dumps(d): df_use_datetime['time'][row].timestamp()})
    redis_insert_end_time = time.perf_counter()
    pipe.execute()

    print("insert to redis done")
    
    #print("len of dataframe", len(df_use_datetime))
    #print(df_use_datetime['time'][0])
    #print(df_use_datetime['time'][len(df_use_datetime)-1])

    score_min = df_use_datetime['time'][0].timestamp()
    score_max = df_use_datetime['time'][len(df_use_datetime)-1].timestamp()

        #elasped_redis_insert = redis_insert_end_time - redis_start_time
        #redis_insert_list.append(elasped_redis_insert)
        #redis_insert_array = np.array(redis_insert_list)

    redis_read_start_time = time.perf_counter()
    #print("number of entries read from redis:")
    #print(r.zcount("openforecast", score_min, score_max))  ## scores are timestamp float
    #print(r.hmget("2017-01-01 00:00:09.554728+00:00", "time", "P1"))
    #print(r.keys())
    #print("print with certain range using zrangebyscore:")
    results_from_redis = r.zrangebyscore("openforecast", score_min, score_max)
    #pipe.execute()
    #print(results_from_redis)
    count_redis_read = 0
    for result_from_redis in results_from_redis:
        #print(pickle.loads(result_from_redis))
        #print(r.hgetall(result_from_redis))
        #print(r.hvals(result_from_redis))
        recordtime, P1, P2, durP1, durP2, humidity, lat, location, lon, ratioP1, ratioP2, sensor_id, sensor_type, temperature = pickle.loads(result_from_redis)
        #[for hash]: recordtime, P1, P2, durP1, durP2, humidity, lat, location, lon, ratioP1, ratioP2, sensor_id, sensor_type, temperature = r.hvals(result_from_redis)
        #pipe.execute()
        lat, lon = float(lat), float(lon)
        if (49 > lat >= 47 and 10 > lon >= 8):
            #print("get filtered:", recordtime, P1, P2, durP1 ,durP2, humidity, lat, location, lon, ratioP1, ratioP2, sensor_id, sensor_type, temperature)
            count_redis_read += 1

    print("number of filtered entries read from redis:", count_redis_read)

    start_query = datetime.fromtimestamp(score_min)
    end_query = datetime.fromtimestamp(score_max)

    redis_read_end_time = time.perf_counter()
    #print("print certain key value pairs:")
    #for query in np.arange(score_min, score_max, 0.000001):
        #r.hmget("query", "lat", "lon")

    #final_result = r.zrange("openforecast",0 ,-1)
    #print(final_result)
    #for j in final_result:
        #pickle.loads(j)

    #redis_read_end_time = time.perf_counter()

        #elasped_redis_return = redis_read_end_time - redis_read_start_time
        #redis_return_list.append(elasped-redis_return)
        #redis_return_array = np.array(redis_return_list)
#for b in final_result:
    #print(pickle.loads(b))

    influx_query_time = influx_query_end_time - influx_query_start_time
    redis_insert_time = redis_insert_end_time - redis_insert_start_time
    redis_read_time = redis_read_end_time - redis_read_start_time

    #query = []
    #insert = []
    #read = []

    query.append(influx_query_time)
    insert.append(redis_insert_time)
    read.append(redis_read_time)

    print("InfluxDB query: %s seconds" % (influx_query_end_time - influx_query_start_time))
    print("Insert to redis: %s seconds "% (redis_insert_end_time - redis_insert_start_time))
    print("Return from redis %s seconds" % (redis_read_end_time - redis_read_start_time))
    print("Number of records: %s" % len(results))

    r.delete("openforecast")

    print("")
    
print("Mean value of influx query time: % s" % (statistics.mean(query)))
print("Mean value of redis insert time: % s" % (statistics.mean(insert)))
print("Mean value of redis read time: % s" % (statistics.mean(read)))

print("Std of influx query time: % s" % (statistics.stdev(query)))
print("Std of redis insert time: % s" % (statistics.stdev(insert)))
print("Std of redis read time: % s" % (statistics.stdev(read)))

