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
import inflect

import psutil
import pymongo
import time
from datetime import datetime

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
        point['sensor_id'] = int(point['sensor_id'])
        #print(type(point['sensor_id']))
        results.append(point)
        #print(point)

    print(len(results))
    lenOfRes = len(results)
    return results, lenOfRes

# data transformation function. To insert data into Redis, data types need to be specified
def transformResults(results):
    #print(results[1])
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

    #df_use['time'] = df_use['time'].astype(str)

    #df_use.reset_index(inplace=True)
    #df_use.head()
    df_use_dict = df_use.to_dict('records')
    #print(df_use_dict)

    df_use_list = df_use.values.tolist()

    #for row in df_use.index:
        #df_use['time'][row] = str(df_use_datetime['time'][row].timestamp())
    #print(df_use)

    return df_use_dict, df_use_list, df_use, df_use_datetime

def toMongo(results, lenOfRes, query_start, num_0):

    print(results)
    print("type of the data", type(results[3]["P1"]))
    print(sys.getsizeof(results[3]["P1"]))
    client = pymongo.MongoClient(host="localhost", port=27017)
    openforecast_db = client.openforecast_db
    openforecast_col = openforecast_db.openforecast_col
    
    #openforecast_col.drop()

    p = inflect.engine()
    length = len(results)
    #print(query_start)
    num = num_0
    #num += lenOfRes
    #timestamp = results[num]["time"]
    
    #timestamp = results[num]["time"]
    #time_obj = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S%z")
    #print("obj:", time_obj)
    # convert timestamp string
    
    # bulk insertion
    # each data entry as a document
    # append each data entry to a list
    # specify entry index as id
    document = []

    count = 0
    while count < length:
        #print(num)
        #print(results[num])
        newResults = {"_id" : num}
        newResults.update(results[count])
        #print(newResults)
        document.append(newResults)
        #print(document)
        #print(num)
        count += 1
        num += 1
        #print(num)        
    openforecast_db.openforecast_col.insert_many(document) 
    
    #outfile = open('pagination.csv', "w")
    #writer = csv.writer(outfile)
    #writer.writerow(dateStr)
    #writer.writerow(num)
    print("num: ", num)
    return num
    ''' 
    #format = "%Y-%m-%d"
    format = "%Y-%m-%dT%H:%M:%S%z"
    # define time range of data insertion
    start_date1 = datetime(2018, 1, 4, 0, 0, 0)
    end_date1 = datetime(2018, 1, 5, 0, 0, 0)
    test_start = start_date1.strftime('%Y-%m-%dT%H:%M:%SZ')
    test_end = end_date1.strftime('%Y-%m-%dT%H:%M:%SZ')
    #print(test)
    query_start1 = str(start_date1)
    query_end1 = str(end_date1)

    inputDate1 = query_start1
    inputDate2 = query_end1
    print("query:", inputDate1)
    
    #for aaa in openforecast_col.find():
        #print(aaa)
    result_cursor = openforecast_col.aggregate([
        { '$match': {'$and' : [ { 'lat': { '$gte': 49, '$lte': 50 } }, { 'time': { '$gte': test_start, '$lte': test_end } }] } }
    ])
    for res in result_cursor:
        print(res)

    '''
    # size bucket insertion
    #while num < length:
        #timestamp = results[num]["time"]
        #time_obj = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S%z")
        #print("obj:", time_obj)
        #print(num)
        #openforecast_db.openforecast_col.update_one(
                #{'nmeasurements': {'$lt': 56100}},
                #{
                #    '$push': {'measurements' : results[num]},
                #    '$min': {'first': results[num]['time']},
                #    '$max': {'last': results[num]['time']},
                #    '$inc': {'nmeasurements': 1}
                #    },
                #upsert=True
                #)
        #num += 1
    
    ''' 
    #for doc in openforecast_col.find():
        #print(doc)

    #cursor = openforecast_col.aggregate([
            #{ '$unwind' : '$measurements'},
            #{ '$elemMatch' : {'lat' : {"$gt": 43}}}
            #])
            #{"lat": {"$gt": 50}}})
    
    #format = "%Y-%m-%d"
    format = "%Y-%m-%dT%H:%M:%S%z"
    # define time range of data insertion
    start_date1 = datetime(2018, 1, 4, 0, 0, 2)
    end_date1 = datetime(2018, 1, 4, 0, 0, 0)
    query_start1 = str(start_date1)
    query_end1 = str(end_date1)
    
    inputDate1 = query_start1
    inputDate2 = query_end1
    print("query:", inputDate1)
    #cursor1 = openforecast_col.find({}, { 'measurements': { '$elemMatch': { 'lat': { '$gt': 40}}}})

    cursor2 = openforecast_col.aggregate([
        {'$match':{ 
            '$expr': {'$and':
            [
                { '$lte': ["$timestamp", { '$toDate': inputDate1 }]},
                { '$gte': ["$timestamp", { '$toDate': inputDate2 }]}
            ]
            }
            }},
        {
            '$project':{
                'measurements': {
                    '$filter': {
                        'input': "$measurements",
                        'as': "measurement",
                        'cond': { 
                            '$and': [
                                { '$gte': [ "$$measurement.lat", 49 ] },
                                { '$lte': [ "$$measurement.lat", 50 ] }
                                ]
                            }
                        }
                    }
                }
            },
        { '$unwind' : '$measurements'},
        { '$project' : { '_id' : 0, 'measurements.lat' : 1, 'measurements.lon' : 1 }},
        ])

    for doc in cursor2:
        print(doc)

    #for ent in res_que:
        #print(ent)
    '''
    #openforecast_col.drop()
    
def main():
    
    client = pymongo.MongoClient(host="localhost", port=27017)
    openforecast_db = client.openforecast_db
    openforecast_col = openforecast_db.openforecast_col

    openforecast_col.drop()

    r = redis.Redis(host='localhost', port=6379, db=0)
    um = 0

    start_time = str('2018-01-01')
    format = "%Y-%m-%d %H:%M:%S"

    # define time range of data insertion
    now = datetime.now()
    start_date = datetime(2015, 10, 12, 0, 0, 0)
    end_date = datetime(2015, 10, 20, 0, 0, 0)
    #end_date = now - timedelta(1)
    print("time of first data entry ", start_date)
    print("time of data of last day", end_date)
    #start_date = date(2018, 1, 30)
    #end_date = date(2018, 1, 31)
    delta_date = (end_date - start_date).days

    # define lists for recording time spend on operations
    fromInfluxDB = [] # time spend on data querying from InfluxDB
    toMongo_time = [] # time spend on data insertion to Redis
    mem_use = [] # number of memory usage after data insertion to Redis
    dateStr = [] # list of date, will be used in .csv files
    numList = []

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
            query_end_csv = (start_date+timedelta(1)).strftime("%Y-%m-%d")

            print("\n\n\nefwgregergergeg", query_end_csv)

            # append each date to the above defined list
            dateStr.append(query_end_csv)
            print("query from: ", query_start)
            print("to: ", query_end)

            # execution time of data querying from InfluxDB
            fromInfluxDB_start = time.perf_counter()
            results, lenOfRes = frominfluxDB(start=query_start, end=query_end)
            fromInfluxDB_end = time.perf_counter()
            print("Query from influxDB done")
            influx_time = fromInfluxDB_end - fromInfluxDB_start
            print("influx query time", influx_time)
            fromInfluxDB.append(influx_time)

            # execution time of data transformation function
            #trans_time_start = time.perf_counter()
            #df_use_dict, df_use_list, df_use, df_use_datetime = transformResults(results)
            #trans_time_end = time.perf_counter()
            #print("trans time: ", (trans_time_end - trans_time_start))

            # execution time of data insertion into Redis
            mongo_insert_start = time.perf_counter()
            #toRedisHash(results, df_use_list, df_use_dict, df_use, df_use_datetime)
            if len(results) != 0:
                num = toMongo(results, lenOfRes, query_start, num_0 = 0)
            numList.append(num)

            #toRedisMultiSortedSet(results, df_use, df_use_datetime)
            mongo_insert_end = time.perf_counter()
            print("insert to MongoDB done")
            mongo_time = mongo_insert_end - mongo_insert_start
            print("time spent inserting into MongoDB", mongo_time)
            toMongo_time.append(mongo_time)

            # the end time of this iteration will be the start time for next iteration
            query_start_mod = query_end
            i += 1
            print(fromInfluxDB)
            print(toMongo_time)

            #newum = r.info()['used_memory']
            #if newum != um and um != 0:
            #print('%d bytes (%d difference)' % (newum, newum - um))
            #um = newum


        elif i > 1:
            #print("need to mod", query_start_mod)
            #print(type(query_start_mod))
            
            query_start_mod = datetime.strptime(query_start_mod, '%Y-%m-%d %H:%M:%S')
            query_end = query_start_mod+timedelta(1)
            #query_start_mod = datetime.strptime(query_start_mod, '%Y-%m-%d %H:%M:%S')
            #query_start_mod = str(query_start_mod)
            #query_end = str(query_end)
            query_end_csv = (query_start_mod+timedelta(1)).strftime("%Y-%m-%d")
            query_start_mod = str(query_start_mod)
            query_end = str(query_end)
            print("\n\n\nefwgregergergeg", query_end_csv)
            #print("str: ", query_start_mod)
            #query_end = str(query_start_mod+timedelta(1))
            #query_end = str(datetime.strptime(query_start_mod, '%Y-%m-%d %H:%M:%S').date()+timedelta(1))
            
            dateStr.append(query_end_csv)
            print("query from: ", query_start_mod)
            print("to: ", query_end)
            #dateStr.append(query_start_mod)
            #dateStr.append(query_end)
            # execution time of data querying from InfluxDB
            fromInfluxDB_start = time.perf_counter()
            results, lenOfRes = frominfluxDB(start=query_start_mod, end=query_end)
            fromInfluxDB_end = time.perf_counter()
            print("Query from influxDB done")
            influx_time = fromInfluxDB_end - fromInfluxDB_start
            fromInfluxDB.append(influx_time)

            # perform the data transformation function
            #df_use_dict, df_use_list, df_use, df_use_datetime = transformResults(results)

            # execution time of data insertion into Redis
            #if len(results) != 0:
            mongo_insert_start = time.perf_counter()
            if len(results) != 0:
                num = toMongo(results, lenOfRes, query_start, num_0 = num)
            numList.append(num)
                #toRedisHash(results, df_use_list, df_use_dict, df_use, df_use_datetime)
                #toRedisMultiSortedSet(results, df_use, df_use_datetime)
            mongo_insert_end = time.perf_counter()
            print("insert to MongoDB done")
            mongo_time = mongo_insert_end - mongo_insert_start
            print("time spent inserting into MongoDB", mongo_time)
            toMongo_time.append(mongo_time)

                # the end time of this iteration will be the start time for next iteration
            query_start_mod = query_end

            i += 1

            print(fromInfluxDB)
            print(toMongo_time)

            #newum = r.info()['used_memory']
            #if newum != um and um != 0:
            #print('%d bytes (%d difference)' % (newum, newum - um))
            #um = newum

        db_stats = openforecast_db.command("dbstats")
        db_size_GB = round(float((db_stats['totalSize'])) /1024/1024/1024, 5)
        disk_cap_GB = round(float((db_stats['fsTotalSize'])) /1024/1024/1024, 5)
        data_occup_rate = (float((db_stats['totalSize'])) / float(db_stats['fsTotalSize'])) * 100
        mem_use.append(data_occup_rate)

        print("database stats: ", db_stats)
        print("database size [GB]: ", db_size_GB)
        print("disk capacity [GB]: ", disk_cap_GB)
        print("data occupancy rate [%]: ", data_occup_rate)

        '''
        print("db stats: ", openforecast_db.command("dbstats"))
        print("db size (GB):" , round(float((openforecast_db.command("dbstats")['totalSize']))/1024/1024/1024, 5))
        print("disk capacity (GB): ", round(float((openforecast_db.command("dbstats")['fsTotalSize']))/1024/1024/1024, 5))
        print("data occupancy rate: ", float((openforecast_db.command("dbstats")['totalSize']))/float(openforecast_db.command("dbstats")['fsTotalSize']))
        '''
        #print("Collection-level stats: " ,openforecast_db.command("collstats", "openforecast_col"))
        
        #print('RAM memory % used:', psutil.virtual_memory()[2])
        
        # write time spend on data insertion into Redis to .csv
        outfile_0 = open('pagination.csv', "w")
        writer = csv.writer(outfile_0)
        writer.writerow(dateStr)
        writer.writerow(numList)
        
        # records time spent on data insertion into MongoDB
        outfile_1 = open('insert_time.csv', "w")
        writer = csv.writer(outfile_1)
        writer.writerow(dateStr)
        writer.writerow(toMongo_time)

        # records memory usage by data in MongoDB
        outfile_2 = open('memUsage_Mongo.csv', "w")
        writer = csv.writer(outfile_2)
        writer.writerow(dateStr)
        writer.writerow(mem_use)

'''
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
        outfile = open('timeSpend_insert_multiSortedSet_row.csv', "w")
        writer = csv.writer(outfile)
        writer.writerow(dateStr)
        writer.writerow(toRedis)

        # write number memory usage into .csv
        outfile = open('memUsage_insert_multiSortedSet_row.csv', "w")
        writer = csv.writer(outfile)
        writer.writerow(dateStr)
        writer.writerow(memory_usage)

    print("Insertion finish!")
'''
main()
