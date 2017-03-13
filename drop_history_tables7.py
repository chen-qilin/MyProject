# -*- coding: utf-8 -*-
# /usr/bin/env python

'''
    确认环境上装有 python 2.7.x 版本
    此脚本依赖python第三方库：pymysql
'''


__Author__ = 'chenqilin'


import re,sys,os
import time
import pymysql
import logging
import fcntl
import datetime
import stat

server_vip = '172.16.131.48' #在引号里填写集群vip
mysql_username = 'root'  #在引号里填写mysql用户名
mysql_passwd = '123456'  #在引号里填写mysql密码
lockfile = 'drop_history_tables.lock'
logfile = 'drop_history_tables.log'
config_file="/infinityfs1/hivedata-bak/config/hive_backup.cfg"
time_file='time'



def newcmp(a,b):
    #定义函数判断日期前的字符是否相同
    a_re = re.match(r'^(\w+)_(20[0-9]{4})$', a)
    b_re = re.match(r'^(\w+)_(20[0-9]{4})$', b)
    if a_re.group(1) != b_re.group(1):
        return a

def read_config(config_file,item):
    content=''
    try:
        with open(config_file,'r') as fcon:
            lines=fcon.readlines()
            for line in lines:
                if item in line:
                    content=re.split('=',line)[1].strip().replace('\n','').replace('\t','')
                    break
    except IOError as e:
        logging.error(e)
    return content

def drop_history_tables():
    # 建立连接
    try:
        connection = pymysql.connect(host=server_vip, port=3306, user=mysql_username, passwd=mysql_passwd, db='information_schema')
    except Exception as e:
        print("Connect Failed---->%s" % e)
        logging.error("%s", e)
        sys.exit(1)

    with connection.cursor() as cur:
        # 查询历史数据表(所有日期结尾的表)
        try:
            count = cur.execute("select TABLE_NAME,TABLE_SCHEMA,concat(round(DATA_LENGTH/1024/1024,2),'MB') as data from tables where table_schema='hivedb' and TABLE_NAME REGEXP '^.+_20[0-9]{4}$'ORDER BY TABLE_NAME;")
        except Exception as e:
            print("Execute Failed---->%s" % e)
            logging.error("%s", e)
            sys.exit(1)
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


        #找出新表和旧表
        new_list = []
        old_list = all_list[:]
        for i in range(count-1):
            j = i+1
            tmp = newcmp(all_list[i],all_list[j])
            if tmp:
                new_list.append(tmp)
                old_list.remove(tmp)

        try:        
            new_list.append(all_list[-1:][0])
            old_list.remove(all_list[-1:][0])
        except IndexError as e:
            logging.error("%s", e)
            return False
            

        print("new_list---->%s" % new_list)
        print("old_list---->%s" % old_list)
        logging.info("new_list---->%s", new_list)
        logging.info("old_list---->%s", old_list)

        
        #删除历史数据表(所有日期结尾的表，保留最近一个月的数据)
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


        #关闭连接
        connection.close()
        logging.debug(100*"*")




def get_excute_time(time_file,new_excute_time):
    excute_time=''
    status=False
    #如果文件存在且记录的内容正确则置标志位True
    if os.path.exists(time_file):
        os.chmod(time_file, stat.S_IRWXU|stat.S_IRWXG|stat.S_IRWXO)
        try:
            with open(time_file,'r+') as fcon:
                excute_time=fcon.read().strip().replace('\n','').replace('\t','')
                match_result=re.match('\d{4}(-\d{2}){5}',excute_time)
                if match_result==None:
                    logging.info("The excute time recored on the time file is not matched, write the current time as the new excute time to it.")
                else:
                    status=True
                #fcon.write(new_excute_time)
        except IOError as e:
            logging.error("Some error occured when read the excute_time file. ",e)
    else:
        logging.info("The excute time is not exists. Make file now")
        try:
            with open(time_file,'w+') as fcon:
                fcon.write(new_excute_time)
        except IOError as e:
            logging.error("Some error occured when write the excute_time file. ",e)
    return status,excute_time






    
if __name__ == '__main__':
    #日志设置
    logging.basicConfig(filename=logfile,
                        filemode='a',
                        level=logging.DEBUG,
                        format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')

    logging.debug(100*"*")

    ####读取配置文件
    BACKUP_ROOT_PATH= read_config(config_file,'HIVE_BACKUP_ROOT_PATH')
    rate_day=int(read_config(config_file,'MYSQL_CLEAR_RATE'))
    clear_time=read_config(config_file,'MYSQL_CLEAR_TIME').replace(':','-')
    new_excute_time=datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    print(rate_day,clear_time,new_excute_time)

    
    while True:
        time.sleep(3600)
        with open(lockfile, 'w') as f:
            try:
	            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except Exception as e:
	        print("Failed to acquire lock")
	        logging.error("Failed to acquire lock!-->%s", e)
	        fcntl.flock(f,fcntl.LOCK_UN)
	        continue
            #获取时间文件
            status,excute_time=get_excute_time(time_file,new_excute_time)
            print (" old excute_time is :%s"%excute_time)

            
            if status==False:
                conti = drop_history_tables()
                if conti == False:
                    fcntl.flock(f,fcntl.LOCK_UN)
                    continue
            else:
                #处理时间逻辑
                excute_time_day=datetime.datetime.strptime(re.match('(\d{4}-\d{2}-\d{2})-(\d{2}-\d{2}-\d{2})',excute_time).group(1),'%Y-%m-%d')
                print("excute_time_day is %s:" %excute_time_day) 
                time_now_day=datetime.datetime.strptime(datetime.date.today().strftime('%Y-%m-%d'),'%Y-%m-%d')
                print("time_now_day is %s:" %time_now_day) 
                delta_days=(time_now_day-excute_time_day).days
  
                #判断日期是否符合周期配置
                if delta_days >= rate_day:
                        #处理时刻
                        now_time=datetime.datetime.strptime(datetime.datetime.now().strftime('%H-%M-%S'),'%H-%M-%S')
                        print("time now is %s " % now_time)
                        last_time=datetime.datetime.strptime(re.match('(\d{4}-\d{2}-\d{2})-(\d{2}-\d{2}-\d{2})',excute_time).group(2),'%H-%M-%S')
                        print("time now is %s " % last_time)
                                    
                        if 0 <= (now_time-last_time).seconds < 3600:
                            #执行清除历史数据表的操作
                            conti = drop_history_tables()
                            if conti == False:
                                fcntl.flock(f,fcntl.LOCK_UN)
                                continue

                fcntl.flock(f,fcntl.LOCK_UN)
          
