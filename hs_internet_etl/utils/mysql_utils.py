# -*- coding:utf-8 -*-
# Author      : suwei<suwei@yuchen.net.cn>
# Datetime    : 2019-07-18 13:40
# User        : suwei
# Product     : PyCharm
# Project     : Demo_BI
# File        : mysql_utils.py
# Description : 数据库连接工具类
import sys
import pymysql

sys.path.append('..')
import hs_internet_etl.settings as settings


class MySqlHelper:

    def __init__(self):
        # 连接数据库
        self.connect_database()
        pass

    def connect_database(self):
        try:
            self.conn = pymysql.connect(**settings.MYSQL_INFO)
            self.cursor = self.conn.cursor()
            return True
        except Exception as e:
            print('数据库连接失败：', e)
            return False

    def execute_sql(self, sql):
        try:
            self.conn.ping(reconnect=True)
            self.cursor.execute(sql)
            return self.cursor.fetchall()
        except Exception as e:
            print('SQL语句执行失败：', e, '\n\t\t', sql)

    def execute_many_sql(self, sql, l):
        try:
            self.conn.ping(reconnect=True)
            self.cursor.executemany(sql, l)
            return True
        except Exception as e:
            print('SQL语句执行失败：', e, '\n\t\t', sql)

    def close(self):
        if self.conn:
            self.cursor.close()
            self.conn.close()
