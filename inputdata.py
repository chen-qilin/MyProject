# -*- coding: utf-8 -*-

import os
import re
import redis
import time
import random
from datetime import datetime
from redis.sentinel import Sentinel


def find_master():
    '''查找master函数'''
    status = False
    master = ""
    sentinel = Sentinel([('172.16.131.34', 6380), ('172.16.131.37', 6380), ('172.16.131.39', 6380)], socket_timeout=0.1)
    master = sentinel.discover_master('mymaster')

    #print master
    content = re.match(r"^\('(\d+.\d+.\d+.\d+)',", str(master))
    if content !=None:
        status = True
        master = content.group(1)
        return status, master
    else:
        return status, master
    # return content.group(1)

if __name__ == "__main__":
    loop_time = 5
    newkey = 'foo' + datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    while True:
        time.sleep(loop_time)
        status, master = find_master()
        if not status:
            print "ERROR: Can not find the master"
            continue
        print master
        ###############链接redis
        r = redis.StrictRedis(host=master, port=6319, db=1)
        ############## 看连接
        # print(r.set('foo', 'bar'))
        # print(r.get('foo'))
        try:
            print("start insert list-->%s" % datetime.now())
            for i in range(20000):
                r.lpush(newkey, '%10.6f' % time.time())
            print("end insert list-->%s" % datetime.now())
            r.lrange(newkey, 0, random.randint(1, 20000))
        except Exception as e:
            print(e)
            continue

        # break
