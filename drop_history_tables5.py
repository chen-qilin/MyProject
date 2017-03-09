# -*- coding: utf-8 -*-
# /usr/bin/env python

'''
    确认环境上装有 python 2.7.x 版本
    此脚本依赖python第三方库：pymysql
'''

__Author__ = 'chenqilin'

import re, sys, os
import time
import pymysql
import logging

#日志设置
logging.basicConfig(filename='drop_history_tables.log',
                    filemode='a',
                    level=logging.DEBUG,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')

logging.debug(100*"*")



#创建锁文件，为了集群下脚本调度
def write_lock(lock_file):
    #如果文件存在则退出程序，否则创建锁文件
    status=False
    if os.path.exists(lock_file):
        logging.warning("The lock file exists.")
        sys.exit(0)
    else:
        try:
            with open(lock_file,'w') as fcon:
                fcon.write("in locka")
                status=True
        except IOError as e:
            logging.error("%s",e)
    return status
    
print(write_lock('Lock.txt'))
logging.info("Successfully acquire lock!")



# 建立连接
try:
    connection = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='123456', db='information_schema')
except Exception as e:
    print("Connect Failed---->%s" % e)
    logging.error("%s", e)  
    sys.exit(0)

with connection.cursor() as cur:
    # 查询历史数据表(所有日期结尾的表)
    try:
        count = cur.execute("select TABLE_NAME,TABLE_SCHEMA,concat(round(DATA_LENGTH/1024/1024,2),'MB') as data from tables where table_schema='hivedb' and TABLE_NAME REGEXP '^.+_20[0-9]{4}$'ORDER BY TABLE_NAME;")
    except Exception as e:
        print("Execute Failed---->%s" % e)
        logging.error("%s", e)
        sys.exit(0)
    result = cur.fetchall()

    print("The number of historical tables is %d :" % count)
    logging.info("The number of historical tables is %d :", count)
    
    #获取所有历史数据表
    all_list = []
    for i in result:
        logging.info("%s", i)
        all_list.append(i[0])
    print("all_list---->%s" % all_list)
    logging.info("all_list---->%s", all_list)

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
        tmp = newcmp(all_list[i],all_list[j])
        if tmp:
            new_list.append(tmp)
            old_list.remove(tmp)
    new_list.append(all_list[-1:][0])
    old_list.remove(all_list[-1:][0])

    print("new_list---->%s" % new_list)
    print("old_list---->%s" % old_list)
    logging.info("new_list---->%s", new_list)
    logging.info("old_list---->%s", old_list)

    
    # 删除历史数据表(所有日期结尾的表，保留最近一个月的数据)
    if count:
        print("***********************")
        print("Start to drop tables!!!")
        logging.warning("Start to drop!!!")
        for i in old_list:
            deletetables = cur.execute("DROP TABLE hivedb.%s" % i)
        print("Drop table complete.")
        logging.warning("Drop table complete.")
        print("***********************")
    else:
        print("There are no tables to delete.")
        logging.warning("There are no tables to delete.")

#删除锁文件
time.sleep(10)
if os.path.exists('Lock.txt'):
    try:
        os.remove('Lock.txt')
    except Exception as e:
        logging.error("Delete lockfile failed!-->%s", e)
    else:
        logging.info("Delete lockfile successfully.")


#关闭连接
connection.close()
logging.debug(100*"*")
