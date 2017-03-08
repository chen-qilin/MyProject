# -*- coding: utf-8 -*-
# /usr/bin/env python

'''
    此脚本依赖python第三方库：pymysql
'''
__Author__ = 'chenqilin'

import re, sys
import pymysql

# 建立连接
try:
    connection = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='123456', db='information_schema')
except Exception as e:
    print("Connect Failed---->%s" % e)
    sys.exit(0)

with connection.cursor() as cur:
    # 查询历史数据表(所有日期结尾的表)
    try:
        count = cur.execute("select TABLE_NAME,TABLE_SCHEMA,concat(round(DATA_LENGTH/1024/1024,2),'MB') as data from tables where table_schema='hivedb' and TABLE_NAME REGEXP '^.+_20[0-9]{4}$'ORDER BY TABLE_NAME;")
    except Exception as e:
        print("Execute Failed---->%s" % e)
        sys.exit(0)
    result = cur.fetchall()

    print("The number of historical tables is %d :" % count)
    
    #打印历史数据表
    all_list = []
    for i in result:
        all_list.append(i[0])
    print("all_list---->%s" % all_list)

    #定义函数判断日期前的字符是否相同
    def newcmp(a,b):
        a_re = re.match(r'^(\w+)_(20[0-9]{4})$', a)
        b_re = re.match(r'^(\w+)_(20[0-9]{4})$', b)
        if a_re.group(1) != b_re.group(1):
            return a

    #找出新表和旧表
    new_list = []
    old_list = all_list[:]
    for i in range(count-1):
        j = i+1
##        try:
        tmp = newcmp(all_list[i],all_list[j])
##        except IndexError:
##            new_list.append(all_list[i])
##            old_list.remove(all_list[i])
##            break
        if tmp:
            new_list.append(tmp)
            old_list.remove(tmp)
    new_list.append(all_list[-1:][0])
    old_list.remove(all_list[-1:][0])

    print("new_list---->%s"%new_list)
    print("old_list---->%s"%old_list)

    
    # 删除历史数据表(所有日期结尾的表，保留最近一个月的数据)
    if count:
        print("***********************")
        print("Start to drop!!!")
        for i in old_list:
            deletetables = cur.execute("DROP TABLE hivedb.%s" % i)
        print("Drop table complete.")
        print("***********************")
    else:
        print("There are no tables to delete.")

#关闭连接
connection.close()
