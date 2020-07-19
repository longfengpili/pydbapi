# @Author: chunyang.xu
# @Email:  398745129@qq.com
# @Date:   2020-06-10 14:40:50
# @Last Modified time: 2020-06-28 11:21:27
# @github: https://github.com/longfengpili

#!/usr/bin/env python3
# -*- coding:utf-8 -*-


import pymysql

from pydbapi.db import DBCommon, DBFileExec
from pydbapi.sql import SqlCompile
from pydbapi.conf import AUTO_RULES


import logging
import logging.config
from pydbapi.conf import LOGGING_CONFIG
logging.config.dictConfig(LOGGING_CONFIG)
mysqllogger = logging.getLogger('mysql')

class SqlMysqlCompile(SqlCompile):
    '''[summary]
    
    [description]
        构造mysql sql
    Extends:
        SqlCompile
    '''
    
    def __init__(self, tablename):
        super(SqlMysqlCompile, self).__init__(tablename)

    def create(self, columns, indexes):
        sql = self.create_nonindex(columns)
        if indexes and not isinstance(indexes, list):
            raise TypeError(f"indexes must be a list !")
        if indexes:
            indexes = ','.join(indexes)
            sql = f"{sql.replace(r');', '')},index({indexes}));"
        return sql

    def add_columns(self, col_name, col_type):
        col_type = f"{col_type}(32)" if col_type == 'varchar' else col_type
        sql = f'alter table {self.tablename} add column {col_name} {col_type};'
        return sql


class MysqlDB(DBCommon, DBFileExec):

    def __init__(self, host, user, password, database, port='3306', charset="utf8"):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.charset = charset
        super(MysqlDB, self).__init__()
        self.auto_rules = AUTO_RULES
    
    def get_conn(self):
        conn = pymysql.connect(database=self.database, user=self.user, password=self.password, host=self.host, port=self.port, charset=self.charset)
        if not conn:
            self.get_conn()
        return conn

    def create(self, tablename, columns, indexes=None):
        # tablename = f"{self.database}.{tablename}"
        sqlcompile = SqlMysqlCompile(tablename)
        sql_for_create = sqlcompile.create(columns, indexes)
        rows, action, result = self.execute(sql_for_create)
        return rows, action, result

    def add_columns(self, tablename, columns):
        old_columns = self.get_columns(tablename)
        old_columns = set(old_columns)
        new_columns = set(columns)
        # mysqllogger.info(f'{old_columns}, {new_columns}')

        if old_columns == new_columns:
            mysqllogger.info(f'【{tablename}】columns not changed !')
        if old_columns - new_columns:
            raise Exception(f"【{tablename}】columns【{old_columns - new_columns}】 not set, should exists !")
        if new_columns - old_columns:
            sqlcompile = SqlMysqlCompile(tablename)
            add_columns = new_columns - old_columns
            for col_name in add_columns:
                col_type = columns.get(col_name)
                sql = sqlcompile.add_columns(col_name, col_type)
                self.execute(sql)
            mysqllogger.info(f'【{tablename}】add columns succeed !【{new_columns - old_columns}】')
