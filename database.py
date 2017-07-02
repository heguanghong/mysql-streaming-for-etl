# -*- coding: UTF-8 -*-


import mysql.connector as mysql
from DBUtils.PooledDB import PooledDB
from encrpt import  *
import  time
class  Database:
        def  __init__(self,host, user,password, db, port=3306, pool=5):
                pwd = decrpt(password,188)
                try:
                        conn_pool = PooledDB(mysql, pool,  host=host,user=user,passwd=pwd,db=db,port=port)
                        self.__conn = conn_pool.connection()
                        self.__cursor = self.__conn.cursor()
                except  Exception,err:
                        print err.args
                        sys.stdout.flush()
                        time.sleep(1)
                        db = Database(host, user,password, db)
                        self.__conn = db.__conn
                        self.__cursor = db.__conn.cursor()

        def  execute(self,sql,value):
                try:
                        self.__cursor.execute(sql,value)
                        return  0
                except  Exception,err:
                        print err.args
                        sys.stdout.flush()
                        if  err.args[0] == 2013 or err.args[0] == 2055:
                                time.sleep(1)
                                self.execute(sql,value)
                        else:
                                exit(1)

        def commit(self):
                self.__conn.commit()



"""
from database import database
db=database(host='10.253.12.146',user='dbmonitor',password='AOONDOLLLNGOEOLMBNFMIMNNANJMINKHMK',db='test')
rct = db.execute("select * from iteminfo limit 2")

"""