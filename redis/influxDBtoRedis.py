import redis
import pandas as pd
import csv
import pickle
import datetime
import subprocess
import json
import influxdb
import time
import sys
import numpy as np
import statistics
from datetime import timedelta, datetime, date
from influxdb import InfluxDBClient, DataFrameClient
from sklearn import preprocessing

#p = inflect.engine()
'''
HOST = os.getenv("HOST")
PORT = '8086'
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
db = os.getenv("DB")
'''
#client = InfluxDBClient(HOST, PORT, USER, PASSWORD, db, ssl=True, verify_ssl=True)
#r = redis.Redis(host='localhost', port=6379, db=0)

#start_time = str('2017-01-01')
#format = "%Y-%m-%d"
#start_date = date(2017, 1, 1)
#end_date = date(2017, 1, 5)
#end_date = date.today()-timedelta(1)
#end_date = str(date.today()-timedelta(1))
#query_times = time(input())

#delta_data = (end_date-start_date).days

#date_params = {'start_time': start_time, 'end_time': end_time}
#data_params = {'start_time': start_time}

#influx_list = []
#redis_insert_list = []
#redis_return_list = []

#print(str(start_date+timedelta(1)))
#for n in range(1, query_times+1):
#results = []
#results_later = []
#i = 1
#while i < delta_date+1:
#    if i ==1:
#start = str(start_date)
#end = str(end_date)

#query = []
#insert = []
#read = []

#end = str(start_date+timedelta(1))

def frominfluxDB(start, end):

    HOST = os.getenv("HOST")
    PORT = os.getenv("PORT")
    USER = os.getenv("USER") 
    PASSWORD = os.getenv("PASSWORD")
    db = os.getenv("DB")
    client = InfluxDBClient(HOST, PORT, USER, PASSWORD, db, ssl=True, verify_ssl=True)

    input_params = {'start': start, 'end': end}
    results = []
    result = client.query('SELECT * FROM sensor WHERE time >= $start AND time < $end', bind_params=input_params)
    points = result.get_points()
    for point in points:
        results.append(point)
    
    print(len(results))
    return results

# data transformation function. To insert data into Redis, data types need to be specified
def transformResults(results):
    df_use = pd.DataFrame(results, columns=['time', 'P1', 'P2', 'durP1', 'durP2', 
                                            'humidity', 'lat', 'location', 'lon', 
                                            'ratioP1', 'ratioP2', 'sensor_id', 'sensor_type', 'temperature'])

    df_use = df_use.fillna(0)
    df_use['P1'] = df_use['P1'].astype(float)
    df_use['durP1'] = df_use['durP1'].astype(float)
    df_use['durP2'] = df_use['durP2'].astype(float)
    df_use['ratioP1'] = df_use['ratioP1'].astype(float)
    df_use['ratioP2'] = df_use['ratioP2'].astype(float)
    df_use['humidity'] = df_use['humidity'].astype(float)
    df_use['temperature'] = df_use['temperature'].astype(float)
    
    le = preprocessing.LabelEncoder()
    df_use['sensor_type'] = le.fit_transform(df_use.sensor_type.values)
    df_use['sensor_type'] = df_use['sensor_type'].astype(float)
    df_use['time'] = pd.to_datetime(df_use['time'])
    
    pd.set_option("display.max_rows", None, "display.max_columns", None)
    #df_use.index += lenCount
    df_use_datetime = df_use.copy()

    df_use['time'] = df_use['time'].astype(str)
    
    #df_use.reset_index(inplace=True)
    #df_use.head()
    df_use_dict = df_use.to_dict('records')
    #print(df_use_dict)

    df_use_list = df_use.values.tolist()
    
    #for row in df_use.index:
        #df_use['time'][row] = str(df_use_datetime['time'][row].timestamp())
    #print(df_use)

    return df_use_dict, df_use_list, df_use, df_use_datetime

def toRedisSortedset(results, df_use_list, df_use, df_use_datetime):

    r = redis.Redis(host='localhost', port=6379, db=0)
    pipe = r.pipeline()
    for count, values, row in zip(range(len(results)), df_use_list, df_use.index):
        ## insert using pure sorted set
        pipe.zadd("openforecast", mapping={pickle.dumps(values): df_use_datetime['time'][row].timestamp()})
    pipe.execute()
    redis_insert_end = time.perf_counter()
    
def toRedisHash(results, df_use_list, df_use_dict, df_use, df_use_datetime):
    
    r = redis.Redis(host='localhost', port=6379, db=0)
    pipe = r.pipeline()
    
    setLen = r.zcard("openforecast")

    for count, d, row in zip(range(len(results)), df_use_dict, df_use.index):
        #print(df_use[row])
        #pipe.hmset((df_use['time'].iloc[count]), d)
        pipe.hmset(row+setLen, d)
        #pipe.hmset(row, d)
        #pipe.zadd("openforecast", mapping={(df_use['time'].iloc[count]): df_use_datetime['time'][row].timestamp()})
        pipe.zadd("openforecast", mapping={row+setLen: df_use_datetime['time'][row].timestamp()})
        #pipe.zadd("openforecast", mapping={row: row})
    pipe.execute()

def toRedisSortedsetCompositeIndexes(results, df_use_list, df_use_dict, df_use, df_use_datetime):

    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    pipe = r.pipeline()

    #timestamp = df_use_datetime['time'][row].timestamp()
    #P1 = df_use_datetime['P1'][row]
    #r.execute_command('ZADD myindex 0 0056:0028.44:90')
    #r.execute_command('ZADD myindex 0 0034:0011.00:832')

    for count, d, row in zip(range(len(results)), df_use_dict, df_use.index):
        timestamp = df_use_datetime['time'][row].timestamp()
        P1 = df_use['P1'][row]
        #P1 = str(P1)
        
        #P1 = float(P1)
        #P1 = '{:09.3f}'.format(P1)
        P1 = float(P1)
        print(timestamp, type(timestamp)) 
        print(P1, type(P1))
        command = f"ZADD openforecast 0 {timestamp}:{P1}"
        print(command)
        r.execute_command(command)



    #r.zadd("test", {"temp":34, "hum":28, "polu":832})
    #r.zadd("test", {"temp":31, "hum":21, "polu":831})
    #r.zadd("test", {34:"temp", 28:"humidity", 832:"polution"})
    #for count, values, row in zip(range(len(results)), df_use_list, df_use.index):
        ## insert using pure sorted set with composite indexes
        # pipe.zadd("openforecast", mapping={pickle.dumps(values): df_use_datetime['time'][row].timestamp()})
        #pipe.zadd(
    pipe.execute()
    
def toRedisMultiSortedSet(results, df_use, df_use_datetime):
    
    # connect to Redis database
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    # define variable for using Redis pipelining
    pipe = r.pipeline()
    
    setLen = r.zcard("time")

    # iterrate through the processed dataframe
    for count, row in zip(range(len(results)), df_use.index):
        ## insert using pure sorted set
        '''
        pipe.zadd("time" ,mapping={df_use_datetime['time'][row].timestamp(): df_use_datetime['time'][row].timestamp()})
        pipe.zadd("P1", mapping={df_use_datetime['time'][row].timestamp(): df_use['P1'][row]})
        pipe.zadd("P2", mapping={df_use_datetime['time'][row].timestamp(): df_use_datetime['P2'][row]})
        pipe.zadd("durP1", mapping={df_use_datetime['time'][row].timestamp(): df_use_datetime['durP1'][row]})
        pipe.zadd("durP2", mapping={df_use_datetime['time'][row].timestamp(): df_use_datetime['durP2'][row]})
        pipe.zadd("humidity", mapping={df_use_datetime['time'][row].timestamp(): df_use_datetime['humidity'][row]})
        pipe.zadd("lat", mapping={df_use_datetime['time'][row].timestamp(): df_use_datetime['lat'][row]})
        pipe.zadd("location", mapping={df_use_datetime['time'][row].timestamp(): df_use_datetime['location'][row]})
        pipe.zadd("lon", mapping={df_use_datetime['time'][row].timestamp(): df_use_datetime['lon'][row]})
        pipe.zadd("ratioP1", mapping={df_use_datetime['time'][row].timestamp(): df_use_datetime['ratioP1'][row]})
        pipe.zadd("ratioP2", mapping={df_use_datetime['time'][row].timestamp(): df_use_datetime['ratioP2'][row]})
        pipe.zadd("sensor_id", mapping={df_use_datetime['time'][row].timestamp(): df_use_datetime['sensor_id'][row]})
        pipe.zadd("sensor_type", mapping={df_use_datetime['time'][row].timestamp(): df_use_datetime['sensor_type'][row]})
        pipe.zadd("temperature", mapping={df_use_datetime['time'][row].timestamp(): df_use_datetime['temperature'][row]})
        '''
        # several sorted sets will be created, for each column one sorted set
        # use values as scores for sorted sets, and index of dataframe as the member
         
        pipe.zadd("time" ,mapping={row+setLen: df_use_datetime['time'][row].timestamp()})
        if df_use['P1'][row] != 0.0:
            pipe.zadd("P1", mapping={row+setLen: df_use['P1'][row]})
        if df_use['P2'][row] != 0.0:
            pipe.zadd("P2", mapping={row+setLen: df_use_datetime['P2'][row]})
        if df_use['durP1'][row] != 0.0:
            pipe.zadd("durP1", mapping={row+setLen: df_use_datetime['durP1'][row]})
        if df_use['durP2'][row] != 0.0:
            pipe.zadd("durP2", mapping={row+setLen: df_use_datetime['durP2'][row]})
        if df_use['humidity'][row] != 0.0:
            pipe.zadd("humidity", mapping={row+setLen: df_use_datetime['humidity'][row]})
        if df_use['lat'][row] != 0.0:
            pipe.zadd("lat", mapping={row+setLen: df_use_datetime['lat'][row]})
        if df_use['location'][row] != 0.0:
            pipe.zadd("location", mapping={row+setLen: df_use_datetime['location'][row]})
        if df_use['lon'][row] != 0.0:
            pipe.zadd("lon", mapping={row+setLen: df_use_datetime['lon'][row]})
        if df_use['ratioP1'][row] != 0.0:
            pipe.zadd("ratioP1", mapping={row+setLen: df_use_datetime['ratioP1'][row]})
        if df_use['ratioP2'][row] != 0.0:
            pipe.zadd("ratioP2", mapping={row+setLen: df_use_datetime['ratioP2'][row]})
        if df_use['sensor_id'][row] != 0.0:
            pipe.zadd("sensor_id", mapping={row+setLen: df_use_datetime['sensor_id'][row]})
        pipe.zadd("sensor_type", mapping={row+setLen: df_use_datetime['sensor_type'][row]})
        if df_use['temperature'][row] != 0.0:
            pipe.zadd("temperature", mapping={row+setLen: df_use_datetime['temperature'][row]})
        
    pipe.execute()
        
    

def main():
    
    r = redis.Redis(host='localhost', port=6379, db=0)
    um = 0

    start_time = str('2018-01-01')
    format = "%Y-%m-%d"

    # define time range of data insertion
    now = datetime.now()
    start_date = datetime(2015, 10, 12, 0, 0, 0)
    #end_date = datetime(2018, 1, 3, 0, 0, 0)
    end_date = now - timedelta(1)
    print("time of first data entry ", start_date)
    print("time of data of last day", end_date)
    #start_date = date(2018, 1, 30)
    #end_date = date(2018, 1, 31)
    delta_date = (end_date - start_date).days
    
    # define lists for recording time spend on operations
    fromInfluxDB = [] # time spend on data querying from InfluxDB
    toRedis = [] # time spend on data insertion to Redis
    memory_usage = [] # number of memory usage after data insertion to Redis
    dateStr = [] # list of date, will be used in .csv files

    i = 1
    while i < delta_date+1:
        #newum = r.info()['used_memory']
        #if newum != um and um != 0:
            #print('%d bytes (%d difference)' % (newum, newum - um))
        #um = newum
        if i == 1:
            # convert the date to string for querying data from InfluxDB
            query_start = str(start_date)
            query_end = str(end_date)
            query_end = str(start_date+timedelta(1))

            # append each date to the above defined list
            dateStr.append(query_start)
            print(query_start)
            print(query_end)
            
            # execution time of data querying from InfluxDB
            fromInfluxDB_start = time.perf_counter()
            results = frominfluxDB(start=query_start, end=query_end)
            fromInfluxDB_end = time.perf_counter()
            print("Query from influxDB done")
            influx_time = fromInfluxDB_end - fromInfluxDB_start
            print("influx query time", influx_time)
            fromInfluxDB.append(influx_time)

            # execution time of data transformation function
            trans_time_start = time.perf_counter()
            if len(results) != 0:
                df_use_dict, df_use_list, df_use, df_use_datetime = transformResults(results)
            trans_time_end = time.perf_counter()
            print("trans time: ", (trans_time_end - trans_time_start))

            # execution time of data insertion into Redis
            redis_insert_start = time.perf_counter()
            #toRedisHash(results, df_use_list, df_use_dict, df_use, df_use_datetime)
            if len(results) != 0:
                #toRedisMultiSortedSet(results, df_use, df_use_datetime)
                #toRedisHash(results, df_use_list, df_use_dict, df_use, df_use_datetime)
                toRedisSortedset(results, df_use_list, df_use, df_use_datetime)
            redis_insert_end = time.perf_counter()
            print("insert to Redis done")
            redis_time = redis_insert_end - redis_insert_start
            print("redis read time", redis_time)
            toRedis.append(redis_time)
            
            # the end time of this iteration will be the start time for next iteration
            query_start_mod = query_end
            i += 1
            print(fromInfluxDB)
            print(toRedis)
            
            #newum = r.info()['used_memory']
            #if newum != um and um != 0:
            #print('%d bytes (%d difference)' % (newum, newum - um))
            #um = newum
            
            
        elif i > 1:

            query_start_mod = datetime.strptime(query_start_mod, '%Y-%m-%d %H:%M:%S')
            query_end = query_start_mod+timedelta(1)
            #query_start_mod = datetime.strptime(query_start_mod, '%Y-%m-%d %H:%M:%S')
            query_start_mod = str(query_start_mod)
            query_end = str(query_end)


            #query_end = str(datetime.strptime(query_start_mod, '%Y-%m-%d').date()+timedelta(1))
            print(query_start_mod)
            print(query_end)
            dateStr.append(query_start_mod)
            
            # execution time of data querying from InfluxDB
            fromInfluxDB_start = time.perf_counter()
            results = frominfluxDB(start=query_start_mod, end=query_end)
            fromInfluxDB_end = time.perf_counter()
            print("Query from influxDB done")
            influx_time = fromInfluxDB_end - fromInfluxDB_start
            fromInfluxDB.append(influx_time)
            
            # perform the data transformation function
            if len(results) != 0:
                df_use_dict, df_use_list, df_use, df_use_datetime = transformResults(results)
            
            # execution time of data insertion into Redis
            redis_insert_start = time.perf_counter()
            #toRedisHash(results, df_use_list, df_use_dict, df_use, df_use_datetime)
            if len(results) != 0:
                #toRedisMultiSortedSet(results, df_use, df_use_datetime)
                #toRedisHash(results, df_use_list, df_use_dict, df_use, df_use_datetime)
                toRedisSortedset(results, df_use_list, df_use, df_use_datetime)
            redis_insert_end = time.perf_counter()
            print("insert to Redis done")
            redis_time = redis_insert_end - redis_insert_start
            toRedis.append(redis_time)
            
            # the end time of this iteration will be the start time for next iteration
            query_start_mod = query_end 
            
            i += 1

            print(fromInfluxDB)
            print(toRedis)

            #newum = r.info()['used_memory']
            #if newum != um and um != 0:
            #print('%d bytes (%d difference)' % (newum, newum - um))
            #um = newum
        
        # print memory usage after each iteration, 
        system_memory = r.info()['total_system_memory']
        used_memory = r.info()['used_memory']
        print("System memory: ", system_memory)
        print("Used memory: ", used_memory)
        rate = float(used_memory) / float(system_memory)
        print("Memory usage rate: %.5f" % (rate * 100))
        memory_usage.append(rate * 100)
        print(memory_usage)
        print("-----------------------------------------------------")
        #newum = r.info()['used_memory']
        #if newum != um and um != 0:
            #print('%d bytes (%d difference)' % (newum, newum - um))
        #um = newum
        
        # write time spend on data insertion into Redis to .csv
        outfile = open('redis_timeSpend_insert_pickledSortedSet_row.csv', "w")
        writer = csv.writer(outfile)
        writer.writerow(dateStr)
        writer.writerow(toRedis)

        # write number memory usage into .csv
        outfile = open('redis_memUsage_pickledSortedSet.csv', "w")
        writer = csv.writer(outfile)
        writer.writerow(dateStr)
        writer.writerow(memory_usage)

    print("Insertion finish!")

main()

'''
for i in range(1):
    results = []
    input_params = {'start': start, 'end': end}
    influx_query_start_time = time.perf_counter()
    result = client.query('SELECT * FROM sensor WHERE time >= $start AND time < $end LIMIT 120', bind_params=input_params)
    influx_query_end_time = time.perf_counter()
    points = result.get_points()
    for point in points:
        results.append(point)
'''
#start_mod = end
#i += 1
'''
    print("query from influxdb done")

    df_use = pd.DataFrame(results, columns=['time', 'P1', 'P2', 'durP1', 'durP2', 'humidity', 'lat', 'location', 'lon', 'ratioP1', 'ratioP2', 'sensor_id', 'sensor_type', 'temperature'])

    df_use['time'] = pd.to_datetime(df_use['time'])

    df_use_datetime = df_use.copy()

    df_use['time'] = df_use['time'].astype(str)

    #df_use_dict = df_use.to_dict('records')
    
    df_use_dict = df_use.values.tolist()

    #df_use_dict_tuple = tuple(df_use_dict)

    #print(df_use.dtypes)
    #print(df_use_dict) 
'''
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
    #pipe = r.pipeline()
    #redis_insert_start_time = time.perf_counter()
    #for count, d, row in zip(range(len(results)), df_use_dict, df_use.index):
    #for pairs in df_use_dict.iteritems():
        #pieces.append(pair[1])
        #pieces.append(pair[0])
        #r.hmset("sensor", d)
        #print(d)
        #pipe.zadd(name="sensor", mapping={pickle.dumps(d): d.get("time").timestamp()})
        #print(d.get("time"))
        
        #print(df_use_datetime['time'][row])
        #print(df_use_datetime['time'][row].timestamp())

'''
        ## insert using hash with sorted set
        pipe.hmset((df_use['time'].iloc[count]), d)
        pipe.zadd("openforecast", mapping={(df_use['time'].iloc[count]): df_use_datetime['time'][row].timestamp()})
    #pipe.execute()
'''

        
        ## insert using pure sorted set
        #pipe.zadd("openforecast", mapping={pickle.dumps(d): df_use_datetime['time'][row].timestamp()})
        
    #pipe.execute()
    #redis_insert_end_time = time.perf_counter()

    #print("insert to redis done")

 
