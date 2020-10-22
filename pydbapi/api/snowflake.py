# @Author: chunyang.xu
# @Email:  398745129@qq.com
# @Date:   2020-10-22 16:12:47
# @Last Modified time: 2020-10-22 17:55:44
# @github: https://github.com/longfengpili

# !/usr/bin/env python3
# -*- coding:utf-8 -*-

import snowflake.connector

from pydbapi.db import DBCommon, DBFileExec
from pydbapi.sql import SqlCompile
from pydbapi.conf import AUTO_RULES


import logging
import logging.config
from pydbapi.conf import LOGGING_CONFIG
logging.config.dictConfig(LOGGING_CONFIG)
mysqllogger = logging.getLogger('mysql')

class SqlSnowflakeCompile(SqlCompile):
    '''[summary]
    
    [description]
        构造mysql sql
    Extends:
        SqlCompile
    '''
    
    def __init__(self, tablename):
        super(SqlSnowflakeCompile, self).__init__(tablename)

    def create(self, columns, indexes):
        sql = self.create_nonindex(columns)
        if indexes and not isinstance(indexes, list):
            raise TypeError(f"indexes must be a list !")
        if indexes:
            indexes = ','.join(indexes)
            sql = f"{sql.replace(';', '')} cluster by ({indexes});"
        return sql

    def add_columns(self, col_name, col_type):
        col_type = f"{col_type}(32)" if col_type == 'varchar' else col_type
        sql = f'alter table {self.tablename} add column {col_name} {col_type};'
        return sql


class SnowflakeDB(DBCommon, DBFileExec):

    def __init__(self, user, password, account, warehouse, database, schema):
        self.user = user
        self.password = password
        self.account = account
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        super(SnowflakeDB, self).__init__()
        self.auto_rules = AUTO_RULES
    
    def get_conn(self):
        conn = snowflake.connector.connect(database=self.database, user=self.user, password=self.password, 
            account=self.account, warehouse=self.warehouse, schema=self.schema)
        if not conn:
            self.get_conn()
        return conn

    def create(self, tablename, columns, indexes=None):
        # tablename = f"{self.database}.{tablename}"
        sqlcompile = SqlSnowflakeCompile(tablename)
        sql_for_create = sqlcompile.create(columns, indexes)
        rows, action, result = self.execute(sql_for_create)
        return rows, action, result

    def add_columns(self, tablename, columns):
        old_columns = self.get_columns(tablename)
        old_columns = set(old_columns)
        new_columns = set(columns)
        mysqllogger.info(f'{old_columns}, {new_columns}')

        if old_columns == new_columns:
            mysqllogger.info(f'【{tablename}】columns not changed !')
        if old_columns - new_columns:
            raise Exception(f"【{tablename}】columns【{old_columns - new_columns}】 not set, should exists !")
        if new_columns - old_columns:
            sqlcompile = SqlSnowflakeCompile(tablename)
            add_columns = new_columns - old_columns
            for col_name in add_columns:
                col_type = columns.get(col_name)
                sql = sqlcompile.add_columns(col_name, col_type)
                self.execute(sql)
            mysqllogger.info(f'【{tablename}】add columns succeed !【{new_columns - old_columns}】')
