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
redisClient = redis.StrictRedis(host='localhost', port=6379, password="mypassword", db=0)
    #decode_responses=True

read = []

for i in range(1):
    redis_read_start_time = time.perf_counter()
    print(redisClient.zcard("sensor"))
    final_result = redisClient.zrangebyscore("sensor", min=200000, max=200010, withscores = True)
    print(final_result)
    for j in final_result:
        print(pickle.loads(j))
    redis_read_end_time = time.perf_counter()

    redis_read_time = redis_read_end_time - redis_read_start_time

    read.append(redis_read_time)

    print("Return from redis %s seconds" % (redis_read_end_time - redis_read_start_time))

#print("Mean value of influx query time: % s" % (statistics.mean(query)))
#print("Mean value of redis insert time: % s" % (statistics.mean(insert)))
print("Mean value of redis read time: % s" % (statistics.mean(read)))

#print("Std of influx query time: % s" % (statistics.stdev(query)))
#print("Std of redis insert time: % s" % (statistics.stdev(insert)))
#print("Std of redis read time: % s" % (statistics.stdev(read)))



