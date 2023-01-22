import redis
from time import sleep

r = redis.Redis(host='localhost', port=6379, db=0)
um = 0

while True:
    newum = r.info()['used_memory']
    if newum != um and um != 0:
        print('%d bytes (%d difference)' % (newum, newum - um))
    um = newum
    sleep(1)
