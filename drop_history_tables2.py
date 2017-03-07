# -*- coding: utf-8 -*-
# /usr/bin/env python

__Author__ = 'chenqilin'

import pymysql
import sys
import re


# 建立连接
try:
    connection = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='123456', db='information_schema')
except Exception as e:
    print("Connect Failed-->%s" % e)
    sys.exit(0)

with connection.cursor() as cur:
    # 查询历史数据表(所有日期结尾的表)
    try:
        count = cur.execute("select TABLE_NAME,TABLE_SCHEMA,concat(round(DATA_LENGTH/1024/1024,2),'MB') as data from tables where table_schema='hivedb' and TABLE_NAME REGEXP '^.+_20[0-9]{4}$'ORDER BY TABLE_NAME;")
    except Exception as e:
        print("Execute Failed-->%s" % e)
        sys.exit(0)
    result = cur.fetchall()

    
    print("The number of historical tables is %d :" % count)
    new_list = []
    all_list = []
    #打印历史数据表
    for i in result:
##        print(i)
        all_list.append(i[0])
    print("all_list-->%s" % all_list)


    def newcmp(a,b):
        a_re = re.match(r'^(\w+)_(20[0-9]{4})$', a)
        b_re = re.match(r'^(\w+)_(20[0-9]{4})$', b)
##        print(a_re.groups(),b_re.groups())
        if a_re.group(1) != b_re.group(1):
            return a

#找出新表和旧表
    old_list = all_list[:]
    for i in range(count):
        j = i+1
        try:
            tmp = newcmp(all_list[i],all_list[j])
        except IndexError:
            new_list.append(all_list[i])
            old_list.remove(all_list[i])
            break
        if tmp:
            new_list.append(tmp)
            old_list.remove(tmp)

    print("newlist-->%s"%new_list)
    print("oldlist-->%s"%old_list)

    
    # 删除历史数据表(所有日期结尾的表，保留最近一个月的数据)
    if count:
        print("******************")
        print("Start to drop!!!")
        for i in old_list:
            deletetables = cur.execute("DROP TABLE hivedb.%s" % i)
        print("Drop table complete.")
        print("******************")
    else:
        print("There are no tables to delete.")

#关闭连接
connection.close()
