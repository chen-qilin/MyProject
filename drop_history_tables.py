# -*- coding: utf-8 -*-
# /usr/bin/env python

__Author__ = 'chenqilin'

import pymysql
import sys

# 建立连接
try:
    connection = pymysql.connect(host='172.16.131.48', port=3306, user='root', passwd='123456', db='information_schema')
except Exception as e:
    print("Connect Failed-->%s" % e)
    sys.exit(0)

with connection.cursor() as cur:
    # 查询历史数据表(所有日期结尾的表)
    try:
        count = cur.execute("select TABLE_NAME,TABLE_SCHEMA,concat(round(DATA_LENGTH/1024/1024,2),'MB') as data from tables where table_schema='hivedb' and TABLE_NAME REGEXP '_20[0-9]{4}$';")
    except Exception as e:
        print("Execute Failed-->%s" % e)
        sys.exit(0)
    result = cur.fetchall()
    print("The number of historical tables is %d :" % count)

    #打印历史数据表
    for i in result:
        print(i)
    
    # 删除历史数据表(所有日期结尾的表)
    if count:
        print("******************")
        print("Start to drop!!!")
        for i in result:
            deletetables = cur.execute("DROP TABLE hivedb.%s" % i[0])
        print("Drop table complete.")
        print("******************")
    else:
        print("There are no tables to delete.")

#关闭连接
connection.close()
