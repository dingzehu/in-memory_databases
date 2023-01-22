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
import datetime
from influxdb import InfluxDBClient, DataFrameClient

import pymongo
import time
import datetime
from datetime import datetime
from datetime import timezone

def queryFromRedisSortedset(start_date_string, end_date_string, perf_100):

    r = redis.Redis(host='localhost', port=6379, db=0)

    #start_date_string = "2017-01-01"
    #end_date_string = "2017-01-03"

    #start_date = datetime.datetime.strptime(start_date_string, "%Y-%m-%d")
    #end_date = datetime.datetime.strptime(end_date_string, "%Y-%m-%d")
    
    start_date = datetime.strptime(str(start_date_string), "%Y-%m-%d %H:%M:%S")
    end_date = datetime.strptime(str(end_date_string), "%Y-%m-%d %H:%M:%S")

    score_min = start_date.replace(tzinfo=timezone.utc).timestamp()
    score_max = end_date.replace(tzinfo=timezone.utc).timestamp()

    #score_min = datetime.datetime.timestamp(start_date.replace(tzinfo=datetime.timezone.utc))
    #score_max = datetime.datetime.timestamp(end_date.replace(tzinfo=datetime.timezone.utc))
    
    print("min score:", score_min)
    print("max score:", score_max)
    count_redis_read = 0
    for num in range(0, 100):
        count_redis_read = 0
        perf_start = time.perf_counter()
        results_from_redis = r.zrangebyscore("openforecast", score_min, score_max)

        for result_from_redis in results_from_redis:
            recordtime, P1, P2, durP1, durP2, humidity, lat, location, lon, ratioP1, ratioP2, sensor_id, sensor_type, temperature = pickle.loads(result_from_redis)
            #if (lat >= 48 and lat <= 50 and lon >= 8 and lon <= 10) or (lat >= 50 and lat < 52 and lon >= 13 and lon < 15):
                #P1, P2, humidity, temperature
        #print("get: ", recordtime, P1, P2, durP1 ,durP2, humidity, lat, location, lon, ratioP1, ratioP2, sensor_id, sensor_type, temperature)

            count_redis_read += 1
        #print("number of entries read from Redis:", count_redis_read)
        perf_end = time.perf_counter()
        perf = perf_end - perf_start
        print(perf)
        perf_100.append(perf)
    print("count: ", count_redis_read)
    print("list of run time: ", perf_100)
    mean_perf = sum(perf_100)/len(perf_100)
    print("mean value of run time: " ,mean_perf)

    return perf_100

def queryFromRedisHash(start_date_string, end_date_string, perf_100):

    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

    #start_date_string = "2017-01-01"
    #end_date_string = "2017-01-03"

    #start_date = datetime.datetime.strptime(start_date_string, "%Y-%m-%d")
    #end_date = datetime.datetime.strptime(end_date_string, "%Y-%m-%d")
    start_date = datetime.strptime(str(start_date_string), "%Y-%m-%d %H:%M:%S")
    end_date = datetime.strptime(str(end_date_string), "%Y-%m-%d %H:%M:%S")

    print(start_date)
    # convert datetime object to Unix timestamp
    score_min = start_date.replace(tzinfo=timezone.utc).timestamp()
    score_max = end_date.replace(tzinfo=timezone.utc).timestamp()

    #score_min = datetime.datetime.timestamp(start_date.replace(tzinfo=datetime.timezone.utc))
    #score_max = datetime.datetime.timestamp(end_date.replace(tzinfo=datetime.timezone.utc))

    print("min score:", score_min)
    print("max score:", score_max)
    count_redis_read = 0
    for num in range(0, 100):
        count_redis_read = 0
        perf_start = time.perf_counter()
        results_from_redis = r.zrangebyscore("openforecast", score_min, score_max)
    
        for result_from_redis in results_from_redis:
        #print(pickle.loads(result_from_redis))
        #print(r.hgetall(result_from_redis))
        #print(r.hvals(result_from_redis))
        ## read from hash with sorted set
        #recordtime, P1, P2, durP1, durP2, humidity, lat, location, lon, ratioP1, ratioP2, sensor_id, sensor_type, temperature = pickle.loads(result_from_redis)
        #recordtime, P1, P2, durP1, durP2, humidity, lat, location, lon, ratioP1, ratioP2, sensor_id, sensor_type,
            recordtime, P1, P2, durP1, durP2, humidity, lat, location, lon, ratioP1, ratioP2, sensor_id, sensor_type, temperature = r.hmget(result_from_redis, "time", "P1", "P2", "durP1", "durP2", "humidity", "lat", "location", "lon", "ratioP1", "ratioP2", "sensor_id", "sensor_type", "temperature")
            #print(recordtime, P1, P2, durP1, durP2, humidity, lat, location, lon, ratioP1, ratioP2, sensor_id, sensor_type, temperature)
            count_redis_read += 1
            #lat, lon= r.hmget(result_from_redis, "lat", "lon")
            #P1 = float(P1) 
            #P2 = float(P2) 
            #durP1 = float(durP1) 
            #durP2 = float(durP2)
            #humidity = float(humidity) 
            #lat = float(lat) 
            #location = int(location)
            #lon = float(lon) 
            #ratioP1 = float(ratioP1)
            #ratioP2 = float(ratioP2) 
            #sensor_id = int(sensor_id) 
            #sensor_type = str(sensor_type) 
            #temperature = float(temperature) 
            #print(lat)

            #if (lat >= 48 and lat < 50 and lon >= 8 and lon < 10) or (lat >= 50 and lat < 52 and lon >= 13 and lon < 15):
                #P1, P2, humidity, temperature = r.hmget(result_from_redis, "P1", "P2", "humidity", "temperature")
                #P1, P2, humidity, temperature= r.hmget(result_from_redis, "P1", "P2", "humidity", "temperature")
                #P1, P2, humidity, temperature = r.hmget(result_from_redis, "P1", "P2", "humidity", "temperature")
                #count_redis_read += 1
            #print(lat, lon, temperature)
    
        perf_end = time.perf_counter()
        perf = perf_end - perf_start
        print(perf)
        perf_100.append(perf)
    print("count: ", count_redis_read)
    print("list of run time: ", perf_100)
    mean_perf = sum(perf_100)/len(perf_100)
    print("mean value of run time: " ,mean_perf)

    return perf_100

def queryFromRedisSortedsetCompositeIndexes():

    r = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)
    records = r.execute_command('ZRANGEBYLEX openforecast [1483228832.330391:1000.00 [1483228833.282692:700.00')
    for record in records:
        print(record)
    # read from hash with sorted set

def queryFromRedisZRNAGESTORE(start_date_string, end_date_string):
    # connect to Redis database
    r = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)

    # convert datetime string the datetime object
    start_date = datetime.datetime.strptime(start_date_string, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date_string, "%Y-%m-%d")

    # convert datetime object to Unix timestamp
    score_min = datetime.datetime.timestamp(start_date.replace(tzinfo=datetime.timezone.utc))
    score_max = datetime.datetime.timestamp(end_date.replace(tzinfo=datetime.timezone.utc))
    
    print("Query data with time range: %s to %s" %(start_date, end_date))
    #print("                  P1 range: %s to %s" %(200, 300))
    #print("                 lat range: %s to %s" %(49, 50))

    # get a list timestamp(s) from 'time' sorted set, using defined range of timestamp
    results_from_redis_time = r.zrangebyscore("time", score_min, score_max)
    # get a list timestamp(s) from 'P1' sorted set, using defined range of P1
    #results_from_redis_P1 = r.zrangebyscore("P1", 200, 300)
    # get a list timestamp(s) from 'lat' sorted set, using defined range lat
    #results_from_redis_lat = r.zrangebyscore("lat", 49, 50)

    score_min = int(score_min)
    score_max = int(score_max)

    # query timestamp data from 'time' sorted set using ZRANGESTORE, store the result in 'timetemp'
    command = f'ZRANGESTORE timetemp time {score_min} {score_max} BYSCORE'
    r.execute_command(command)
    #qqq = r.zrange("timetemp", 0, -1)
    
    # query timestamp data from 'P1' sorted set using ZRANGESTORE, store the result in 'P1temp'
    r.execute_command('ZRANGESTORE P1temp P1 200 300 BYSCORE')
    #aaa = r.zrange("P1temp", 0, -1)
    
    # query timestamp data from 'lat' sorted set using ZRANGESTORE, store the result in 'lattemp'
    r.execute_command('ZRANGESTORE lattemp lat 49 50 BYSCORE')
    #zzz = r.zrange("lattemp", 0, -1)

    # get timestamp intersect from above three results
    inters = r.execute_command('ZINTER 3 timetemp P1temp lattemp')
    
    # delete unnecessary sorted sets
    r.execute_command('DEL timetemp')
    r.execute_command('DEL P1temp')
    r.execute_command('DEL lattemp')

    print("final output")
    # # for the following loop, function 'r.score' is used to return the score of member (timestamp)
    # r.zscore('time', inter) is used to return the time value corresponding to value of inter (timestamp),
    # r.zscore('P1', inter) is used to return the P1 value corresponding to value of inter (timestamp), so on ...
    for inter in inters:
        print([r.zscore('time', inter), r.zscore('P1', inter), r.zscore('P2', inter), r.zscore('durP1', inter),
        r.zscore('durP2', inter), r.zscore('humidity', inter), r.zscore('lat', inter), r.zscore('location', inter),
        r.zscore('lon', inter), r.zscore('ratioP1', inter), r.zscore('ratioP2', inter), r.zscore('sensor_id', inter),
        r.zscore('sensor_type', inter), r.zscore('temperature', inter)])
    
def queryFromRedisMultiSortedSet(start_date_string, end_date_string, perf_100):

    # connect to Redis database
    r = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)
    pipe = r.pipeline() 

    # convert datetime string the datetime object
    start_date = datetime.datetime.strptime(start_date_string, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date_string, "%Y-%m-%d")

    # convert datetime object to Unix timestamp
    score_min = datetime.datetime.timestamp(start_date.replace(tzinfo=datetime.timezone.utc))
    score_max = datetime.datetime.timestamp(end_date.replace(tzinfo=datetime.timezone.utc))
    
    print("Query data with time range: %s to %s" %(start_date, end_date))
    print("                 lat range: %s to %s" %(48, 50))
    print("                 lon range: %s to %s" %(8, 10))
    
    # get a list index(es) from 'time' sorted set, using defined range of timestamp
    #perf_100 = []
    for num in range(0, 10):
        perf_start = time.perf_counter()
        
        inters = r.zrangebyscore("time", score_min, score_max)
        countinter = 0
        for inter in inters:
            if r.zscore("lat", inter) != None and r.zscore("lon", inter) != None:
                if (r.zscore("lat", inter) >= 48 and r.zscore("lat", inter) < 50 and r.zscore("lon", inter) >= 8 and r.zscore("lon", inter) < 10): 
                    #or (r.zscore("lat", inter) >= 50 and r.zscore("lat", inter) < 52 and r.zscore("lon", inter) >= 13 and r.zscore("lon", inter) < 15):
                    #pass
                    r.zscore('time', inter),
                    r.zscore('P1', inter),
                    r.zscore('P2', inter),
                    r.zscore('durP1', inter),
                    r.zscore('durP2', inter),
                    r.zscore('humidity', inter),
                    r.zscore('lat', inter),
                    r.zscore('location', inter),
                    r.zscore('lon', inter),
                    r.zscore('ratioP1', inter),
                    r.zscore('ratioP2', inter),
                    r.zscore('sensor_id', inter),
                    r.zscore('sensor_type', inter),
                    r.zscore('temperature', inter)
                    countinter += 1
                    #print(countinter)
                    
                    
                    #print(r.zscore("lat", inter))

        #inters = results_from_redis_time
        #results_from_redis_lat_0 = r.zrangebyscore("lat", 49, 50)
        #results_from_redis_P1 = r.zrangebyscore("P1", 200, 300)
        #results_from_redis_lon_0 = r.zrangebyscore("lon", 10, 12)
        #results_from_redis_lat_1 = r.zrangebyscore("lat", 48, 50)
        #results_from_redis_lon_1 = r.zrangebyscore("lon", -1, 1)
        #results_from_redis_snesorid = r.zrangebyscore("sensor_id", 3102, 3102)
        #time_sensor1 = set(r.zrangebyscore("time", 1514764800, 1515628800)) & set(r.zrangebyscore("sensor_id", 3102, 3102)) 
        #r.zrangebyscore("sensor_id", 6934, 6934)
        #inters = set(r.zrangebyscore("time", score_min, score_max)) & set(r.zrangebyscore("lat", 48, 50)) & set(r.zrangebyscore("lon", 8, 10))
        #for inter in inters:
                #r.zscore('time', inter),
                #r.zscore('P1', inter),
                #r.zscore('P2', inter),
                #r.zscore('durP1', inter),
                #r.zscore('durP2', inter),
                #r.zscore('humidity', inter),
                #r.zscore('lat', inter),
                #r.zscore('location', inter),
                #r.zscore('lon', inter),
                #r.zscore('ratioP1', inter),
                #r.zscore('ratioP2', inter),
                #r.zscore('sensor_id', inter),
                #r.zscore('sensor_type', inter),
                #r.zscore('temperature', inter)

        #inters = r.zrangebyscore("time", 1515542400, 1515628800)
        perf_end = time.perf_counter()
        perf = perf_end - perf_start
        print(perf)
        perf_100.append(perf)

        #for inter in inters:
                #[r.zscore('time', inter),
                #r.zscore('P1', inter),
                #r.zscore('P2', inter),
                #r.zscore('durP1', inter),
                #r.zscore('durP2', inter),
                #r.zscore('humidity', inter),
                #r.zscore('lat', inter),
                #r.zscore('location', inter),
                #r.zscore('lon', inter),
                #r.zscore('ratioP1', inter),
                #r.zscore('ratioP2', inter),
                #r.zscore('sensor_id', inter),
                #r.zscore('sensor_type', inter),
                #r.zscore('temperature', inter)]

    print("list of run time: ", perf_100)
    mean_perf = sum(perf_100)/len(perf_100)
    print("mean value of run time: " ,mean_perf)

    return perf_100
    #print(countinter)
    # obtain data searching range from defined time range
    # create temporary sorted set(s) which has the same index(es) as the list above, and with the corresponding values 
    # to limit the range of querying from 'P1' sorted set and 'lat' sorted set
    #for time in results_from_redis_time:
    #    temp_P1 = r.zscore('P1', time)
    #    if temp_P1 is not None:
    #        r.zadd("P1temp", mapping={time: temp_P1})
    #    temp_lat = r.zscore('lat', time)
    #    if temp_P1 is not None:
    #        r.zadd("lattemp", mapping={time: temp_lat})
    #pipe.execute()
    # get a list index(es) from 'P1temp' temporary sorted set, using defined range of P1
    #results_from_redis_P1 = r.zrangebyscore("P1", 200, 300)
    
    # get a list index(es) from 'lattemp' temporary sorted set, using defined range of lat
    #results_from_redis_lat = r.zrangebyscore("lat", 49, 50)
    
    # delete temporary sorted set(s) after results of data searching written
    #pipe.execute_command('DEL P1temp')
    #pipe.execute_command('DEL lattemp')
    #pipe.execute()
    
    # gets intersect of each above list, to get the list of index(es) that consists of data of interest
    # the list of index(es) (inters) will be used to query data from sorted sets
    #inters = set(results_from_redis_time) & set(results_from_redis_P1) & set(results_from_redis_lat)

    #print("final output:")
    # for the following loop, function 'r.score' is used to return the score of member (index) 
    # r.zscore('time', inter) is used to return the time value corresponding to value of inter (index),
    # r.zscore('P1', inter) is used to return the P1 value corresponding to value of inter (index), 
    # and so on ...
    
    #print(inters)

    '''
    count = 0
    for inter in inters:
        print([r.zscore('time', inter), 
            r.zscore('P1', inter), 
            r.zscore('P2', inter), 
            r.zscore('durP1', inter),
            r.zscore('durP2', inter), 
            r.zscore('humidity', inter), 
            r.zscore('lat', inter), 
            r.zscore('location', inter),
            r.zscore('lon', inter), 
            r.zscore('ratioP1', inter), 
            r.zscore('ratioP2', inter), 
            r.zscore('sensor_id', inter),
            r.zscore('sensor_type', inter), 
            r.zscore('temperature', inter)])
        count += 1
        if count == 15:
            break
    '''
    '''
    for inter in inters:
        print([r.zscore('time', inter), r.zscore('P1', inter), r.zscore('P2', inter), r.zscore('durP1', inter),
        r.zscore('durP2', inter), r.zscore('humidity', inter), r.zscore('lat', inter), r.zscore('location', inter),
        r.zscore('lon', inter), r.zscore('ratioP1', inter), r.zscore('ratioP2', inter), r.zscore('sensor_id', inter),
        r.zscore('sensor_type', inter), r.zscore('temperature', inter)]) 
    ''' 
def main():
    
    format = "%Y-%m-%d"
    # defind start time and end time to return data 
    start_date = date(2017, 7, 1)
    end_date = date(2017, 7, 2)
    delta_date = (end_date - start_date).days

    #format = "%Y-%m-%d"
    format = "%Y-%m-%dT%H:%M:%S%z"
    # define time range of data insertion
    start_date1 = datetime(2018, 1, 1, 0, 0, 0)
    end_date1 = datetime(2018, 1, 1, 1, 0, 0)
    #test_delta = (end_date1 - start_date1).days
    test_delta = (end_date1 - start_date1).total_seconds() / 3600.0
    print("test_delta: ", test_delta)
    test_start = start_date1.strftime('%Y-%m-%dT%H:%M:%SZ')
    test_end = end_date1.strftime('%Y-%m-%dT%H:%M:%SZ')

    # create a list consists of Redis returning time
    fromRedis = []

    # creaet a list consists of dates
    dateStr = []

    i = 1
    perf_final_sum = []
    # query data from the start time until the end time achieved, day by day
    for perf_count in range(0, 1):
        print(perf_count)
        #sum_perf_100 = 0
        i = 1
        while i < test_delta+1:
            if i == 1:
                #query_start = str(start_date)
                #query_end = str(end_date)

                # start time + 1 day -> end time
                #query_end = str(start_date+timedelta(1))

                query_start = start_date1
                #query_start = str(start_date)
                #query_end = str(end_date)
                query_end = start_date1+timedelta(hours = 1)

                #print("query start time: ", query_start)
                #print("query end time: ", query_end)

                # append each date into ths list of dates
                dateStr.append(query_start)
            
                # execution time of the function performing data querying
                fromRedis_start_time = time.perf_counter()
                perf_100 = queryFromRedisSortedset(query_start, query_end, perf_100 = [])
                #perf_100 = queryFromRedisHash(query_start, query_end, perf_100 = [])
                #queryFromRedisSortedsetCompositeIndexes()
                #perf_100 = queryFromRedisMultiSortedSet(query_start, query_end, perf_100 = [])
                sum_perf_100 = sum(perf_100)
                #queryFromRedisZRNAGESTORE(query_start, query_end)
                fromRedis_end_time = time.perf_counter()

                # append execution time into the list of Redis returning time
                fromRedis.append(fromRedis_end_time - fromRedis_start_time)
                print("\ntime spend for querying from Redis [s]:", fromRedis)
            
                # now the start time of next query iteration is the end time of this query iteration
                query_start_mod = query_end

                i += 1
                print("\n\n")

            elif i > 1:
                # end time of last query iteration + 1 -> end time of this query iteration
                #query_end = str(datetime.datetime.strptime(query_start_mod, "%Y-%m-%d").date()+timedelta(1))
                query_end = query_start_mod+timedelta(days = 1)
                #print(query_start_mod)
                #print(query_end)

                # append each date into the list of dates
                dateStr.append(query_start_mod)
            
                # execution time of the function performing data querying
                fromRedis_start_time = time.perf_counter()
                perf_100 = queryFromRedisSortedset(query_start_mod, query_end, perf_100 = perf_100)
                #perf_100 = queryFromRedisHash(query_start_mod, query_end, perf_100 = perf_100)
                #queryFromRedisSortedsetCompositeIndexes()
                #perf_100 = queryFromRedisMultiSortedSet(query_start_mod, query_end, perf_100 = perf_100)
                print("sum list for querying for 10 days: ", perf_100)
                sum_perf_100 = sum(perf_100)
                print("sum time for the list above: ", sum_perf_100)
                #queryFromRedisZRNAGESTORE(query_start_mod, query_end)
                fromRedis_end_time = time.perf_counter()

                # append execution time to the list of Redis returing time
                fromRedis.append(fromRedis_end_time - fromRedis_start_time)
                print("\ntime spend for querying from Redis [s]:", fromRedis)

                i += 1
                # the start time of next query iteration is the end time of this query iteration
                query_start_mod = query_end
                print("\n\n")
        
        perf_final_sum.append(sum_perf_100)
        print(perf_final_sum)
    perf_final = sum(perf_final_sum)/len(perf_final_sum)
    print("perf_final", perf_final)
        # write above mentioned lists to .csv
        #outfile = open('timeSpend_query.csv', "w")
        #writer = csv.writer(outfile)
        #writer.writerow(dateStr)
        #writer.writerow(fromRedis)
main()
