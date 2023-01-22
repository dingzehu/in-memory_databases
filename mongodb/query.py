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

import pymongo
import time
import datetime
from datetime import datetime

def query(query_start, query_end, skipCount, paging, perf_100):
    
    print("received data strings:", query_start, query_end)

    client = pymongo.MongoClient(host="localhost", port=27017)
    openforecast_db = client.openforecast_db
    openforecast_col = openforecast_db.openforecast_col
    
    print("transformed date str: ", query_start.strftime('%Y-%m-%dT%H:%M:%SZ'), query_end.strftime('%Y-%m-%dT%H:%M:%SZ'))

    #format = "%Y-%m-%d"
    format = "%Y-%m-%dT%H:%M:%S%z"
    # define time range of data insertion
    #start_date1 = datetime(2018, 1, 7, 0, 0, 0)
    #end_date1 = datetime(2018, 1, 7, 10, 0, 0)
    start_date1 = query_start
    end_date1 = query_end
    #print("start_date1: ", start_date1)
    test_start = start_date1.strftime('%Y-%m-%dT%H:%M:%SZ')
    test_end = end_date1.strftime('%Y-%m-%dT%H:%M:%SZ')
    #print(test)
    query_start1 = str(start_date1)
    query_end1 = str(end_date1)
    
    test_start_short = start_date1.strftime('%Y-%m-%d')
    print('date from csv:', test_start_short)
    # read paging number from .csv
    csvfile = open('pagination.csv')
    #pagingfile = csv.reader(csvfile)
    #for row in pagingfile:
        #print('row[0]',row[0:2])
        #if row[0]==test_start_short:
            #print('csv test', row[2])
    #datecols = next(pagingfile)
    pagingdata = pd.read_csv("pagination.csv", header=None)
    
    for i in range(len(pagingdata.columns)):
        if str(pagingdata.iat[0,i]) == test_start_short:
            paging = int(pagingdata.iat[1,i])
            break

        #if pagingdata[row] == test_start_short:
            #print('row', pagingdata[row])
    #line = next((x for i, x in enumerate(pagingfile) if i == 0), None)
    #print(line)

    #rows = []
    #for i, row in enumerate(pagingfile):
        #if i == 0:
            #print("This is the line.")
            #print(row)
            #break

    #print(paging)
    inputDate1 = query_start1
    inputDate2 = query_end1
    #print("format for query:", test_start)
    print("query from %s to %s" %(test_start, test_end))
    #for aaa in openforecast_col.find():
        #print(aaa)
    
    
    for num in range(0, 1):
        
        perf_start = time.perf_counter()
        pagingdata = pd.read_csv("pagination.csv", header=None)
        for i in range(len(pagingdata.columns)):
            if str(pagingdata.iat[0,i]) == test_start_short:
                paging = int(pagingdata.iat[1,i])
                break
        
        #print(num)
        #perf_start = time.perf_counter()
        
        '''
        result_cursors_3 = openforecast_col.find(
                    { 'lat': { '$gt': 48, '$lte': 50 } },
                    { 'lon': { '$gt': 10, '$lte': 12 } },
                    { 'time': { '$gte': '2018-01-01T00:00:00Z', '$lte': '2018-01-02T00:00:00Z' } },
                    #{ 'P1': { '$gte': 2, '$lte': 3 } },
                    { '_id': { '$gt': paging} },
                    sort=[("P1", pymongo.DESCENDING)]       
                #{'$limit' : 15},
                #{ '$group' : { '_id' : 'null', 'max_P1' : { '$max' : '$P1' }}}
            #{ '$limit': 15},
            #{ '$project' : { '_id' : 1, 'time' : 1, 'lat' : 1, 'P1' : 1 } },
            #{ '$skip': skipCount}
            #{ '$sort': {'_id' : -1}}
            ])
        '''
        print("paging number: ", paging)
        result_cursors_2 = openforecast_col.aggregate([
            { '$match': {'$and' :
                [
                    { 'lat': { '$gte': 48, '$lte': 50 } },
                    { 'lon': { '$gte': 8, '$lte': 10 } },
                    { 'time': { '$gte': test_start, '$lt': test_end } },
                    #{ 'P1': { '$gte': 2, '$lte': 3 } },
                    { '_id': { '$gt': paging} }
                    ] } },
                #{ '$limit' : 100 },
                { '$group' : { '_id' : 'null', 'avg_val' : { '$avg' : '$temperature' }}}
                #{'$limit' : 10},
                #{ '$sort': { 'temperature' : -1 } },
                #{'$limit' : 1},
                #{ '$group' : { '_id' : '$sensor_id', 'P1' : { '$first' : '$P1' }, 'doc_id' : { '$first' : '$_id' }}},
                #{ '$replaceWith' : '$max_P1' }
            #{ '$limit': 15},
            #{ '$project' : { '_id' : 1, 'temperature' : 1, 'sensor_id' : 1, 'time' : 1 } },
            #{ '$skip': skipCount}
            #{ '$sort': {'_id' : -1}}
            ])
        
        '''
        result_cursors_1 = openforecast_col.aggregate([
            { '$match': {'$and' :
                [
                    #{ 'lat': { '$gt': 50, '$lte': 52, '$gt': 48, '$lte': 50 } },
                    #{ 'lat': { '$gt': 50, '$lte': 52, '$gt': 48, '$lte': 50 } },
                    #{ 'lon': { '$gt': 10, '$lte': 12, '$gt': -1, '$lte': 1} },
                    { '$or':
                        [
                            #{ '$and':
                                #[
                                    {'sensor_id' : { '$eq': 3102 } },
                            #{ '$and':
                                #[
                                    {'sensor_id' : { '$eq': 6934 } } ] } ,
                            #{ '$and':
                                #[
                                    #{'lon' : { '$gt': -1, '$lte': 1 } } ] },
                            #{ '$and':
                                #[
                                    #{'lat' : { '$gt': 10, '$lte': 12 } } ] },
                    #{ 'lat': { '$gt': 50, '$lte': 52 } },
                    #{ 'lon': { '$gt': -1, '$lte': 1 } },
                    { 'time': { '$gte': test_start, '$lte': test_end } },
                    #{ 'P1': { '$gte': 2, '$lte': 3 } },
                    { '_id': { '$gt': paging} }
                    ] } } ,
            #{ '$project' : { '_id' : 1, 'time' : 1, 'lat' : 1, 'P1' : 1 } },
            #{ '$limit' : 1500 },
            { '$project' : {'ratioP1' : 1, 'ratioP2' : 1, 'humidity' : 1, 'temperature' : 1 } },
            #{ '$limit' : 15 },
            #{ '$skip': skipCount}
            #{ '$sort': {'_id' : -1}}
            ])
        '''
        '''
        result_cursors_0 = openforecast_col.aggregate([
            { '$match': {'$and' :
                [
                    #{ 'lat': { '$gt': 50, '$lte': 52, '$gt': 48, '$lte': 50 } },
                    #{ 'lat': { '$gt': 50, '$lte': 52, '$gt': 48, '$lte': 50 } },
                    #{ 'lon': { '$gt': 10, '$lte': 12, '$gt': -1, '$lte': 1} },
                    { '$or':
                        [
                            { '$and':
                                [
                                    {'lat' : { '$gte': 48, '$lt': 50 } } ] },
                            { '$and':
                                [
                                    {'lon' : { '$gte': 8, '$lt': 10 } } ] },
                            { '$and':
                                [
                                    {'lat' : { '$gte': 50, '$lte': 52 } } ] },
                            { '$and':
                                [
                                    {'lon' : { '$gt': 13, '$lte': 15 } } ] }]},
                    #{ 'lat': { '$gt': 50, '$lte': 52 } },
                    #{ 'lon': { '$gt': -1, '$lte': 1 } },
                    { 'time': { '$gte': test_start, '$lte': test_end } },
                    #{ 'P1': { '$gte': 2, '$lte': 3 } },
                    { '_id': { '$gt': paging} }
                    ] }},
            #{ '$project' : { '_id' : 1, 'time' : 1, 'lat' : 1, 'P1' : 1 } },
            #{ '$limit' : 150 },
            { '$project' : {'P1' : 1, 'P2' : 1, 'humidity' : 1, 'temperature' : 1 } },
            #{ '$limit' : 15 },
            #{ '$skip': skipCount}
            #{ '$sort': {'_id' : -1}}
            ])
        '''


        perf_end = time.perf_counter()
        perf = perf_end - perf_start
        perf_100.append(perf)
    perf_avg = sum(perf_100)/len(perf_100)
    print("mean value of querying from MongoDB: %f [s]" % perf_avg)
    
    #with open("/home/cloud/openforecast/mongodb/output", "w") as f:
        #for row in result_cursors_2:
            #f.write(row)

    #forcount = 0
    #for result_cursor in result_cursors_2:
        #pass
        #print(result_cursor)
        #forcount += 1
    #print("fewilfnwiefn", forcount)
    #last_id = result_cursor[-1]
    #print(last_id)
    return paging, perf_100
    '''
    count = openforecast_col.aggregate([
        { '$match': {'$and' :
            [ { 'lat': { '$gte': 49, '$lte': 50 } },
                { 'time': { '$gte': test_start, '$lte': test_end } },
                { 'P1': { '$gte': 2, '$lte': 12 } }
                ] } },
        { '$project' : { '_id' : 1, 'time' : 1, 'lat' : 1, 'P1' : 1 } },
        {"$count": "number of occurrences"}
    ])
    '''
    
    #for res in range(0, len(list(result_cursor))):
        #if res == (len(list(result_cursor))-1):
            #print(last_id = res['_id'])

    #return last_id

    #test = openforecast_col.find()

    #for aaa in test:
        #print(aaa)

    '''
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
    cursor1 = openforecast_col.find({}, { 'measurements': { '$elemMatch': { 'lat': { '$gt': 40}}}})

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
        ])

    for doc in cursor2:
        print(doc)
    '''
def main():

    format = "%Y-%m-%d"
    # defind start time and end time to return data
    start_date = date(2018, 11, 15)
    end_date = date(2018, 1, 2)
    delta_date = (end_date - start_date).days

    #format = "%Y-%m-%d"
    format = "%Y-%m-%dT%H:%M:%S%z"
    # define time range of data insertion
    start_date1 = datetime(2018, 1, 1, 0, 0, 0)
    end_date1 = datetime(2018, 1, 2, 0, 0, 0)
    test_delta = (end_date1 - start_date1).days
    #test_delta = (end_date1 - start_date1).total_seconds() / 600.0
    print("test_delta: ", test_delta)
    test_start = start_date1.strftime('%Y-%m-%dT%H:%M:%SZ')
    test_end = end_date1.strftime('%Y-%m-%dT%H:%M:%SZ')


    # create a list consists of Redis returning time
    fromRedis = []
    
    # creaet a list consists of dates
    dateStr = []

    skipCount = 0
    i = 1
    perf_final_sum = []
    # query data from the start time until the end time achieved, day by day
    for perf_count in range(0, 1):
        print(perf_count)
        #sum_perf_100 = 0
        i = 1
        while i < test_delta+1:
            if i == 1:
                query_start = start_date1
                #query_end = end_date1

                # start time + 1 day -> end time
                query_end = start_date1+timedelta(days = 1)
                #query_end_str = query_end.strftime('%Y-%m-%dT%H:%M:%SZ')
                print("query end: ", query_end)
                #print("query start time: ", query_start)
                #print("query end time: ", query_end)

                # append each date into ths list of dates
                dateStr.append(query_start)
                
                # execution time of the function performing data querying
                #perf_100 = []
                fromRedis_start_time = time.perf_counter()
                paging, perf_100 = query(query_start, query_end, skipCount, paging = 0, perf_100 = [])
                #print(perf_100)
                sum_perf_100 = sum(perf_100)
                #print("sum: ", sum_perf_100)
                #queryFromRedisHash(query_start, query_end)
                #queryFromRedisSortedsetCompositeIndexes()
                #queryFromRedisMultiSortedSet(query_start, query_end)
                #queryFromRedisZRNAGESTORE(query_start, query_end)
                fromRedis_end_time = time.perf_counter()

                # append execution time into the list of Redis returning time
                fromRedis.append(fromRedis_end_time - fromRedis_start_time)
                #print("\ntime spend for querying from Redis [s]:", fromRedis)

                # now the start time of next query iteration is the end time of this query iteration
                query_start_mod = query_end

                i += 1
                skipCount += 3000000
                print("\n\n")

            elif i > 1:
                # end time of last query iteration + 1 -> end time of this query iteration
                query_end = query_start_mod+timedelta(days = 1)
                #query_end = str(datetime.datetime.strptime(query_start_mod, "%Y-%m-%d").date()+timedelta(1))
                
                #test_start = start_date1.strftime('%Y-%m-%dT%H:%M:%SZ')
                #test_end = end_date1.strftime('%Y-%m-%dT%H:%M:%SZ')
                
                #print(query_start_mod)
                #print(query_end)

                # append each date into the list of dates
                dateStr.append(query_start_mod)

                # execution time of the function performing data querying
                fromRedis_start_time = time.perf_counter()
                #paging += 3000000
                paging, perf_100 = query(query_start_mod, query_end, skipCount, paging, perf_100 = perf_100)
                print("sum list for querying for 10 days: ", perf_100)
                sum_perf_100 = sum(perf_100)
                print("sum time for the list above: ", sum_perf_100)
                #queryFromRedisHash(query_start_mod, query_end)
                #queryFromRedisSortedsetCompositeIndexes()
                #queryFromRedisMultiSortedSet(query_start_mod, query_end)
                #queryFromRedisZRNAGESTORE(query_start_mod, query_end)
                fromRedis_end_time = time.perf_counter()

                # append execution time to the list of Redis returing time
                #print("\ntime spend for querying from Redis [s]:", fromRedis)

                i += 1
                skipCount += 3000000
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

