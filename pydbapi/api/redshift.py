# @Author: chunyang.xu
# @Email:  398745129@qq.com
# @Date:   2020-06-10 14:40:50
# @Last Modified time: 2020-06-28 11:21:33
# @github: https://github.com/longfengpili

#!/usr/bin/env python3
# -*- coding:utf-8 -*-


import psycopg2

from pydbapi.db import DBCommon, DBFileExec
from pydbapi.sql import SqlCompile
from pydbapi.conf import REDSHIFT_AUTO_RULES


import logging
import logging.config
from pydbapi.conf import LOGGING_CONFIG
logging.config.dictConfig(LOGGING_CONFIG)
redlogger = logging.getLogger('redshift')

class SqlRedshiftCompile(SqlCompile):
    '''[summary]
    
    [description]
        构造redshift sql
    Extends:
        SqlCompile
    '''
    
    def __init__(self, tablename):
        super(SqlRedshiftCompile, self).__init__(tablename)

    def create(self, columns, indexes):
        sql = self.create_nonindex(columns)
        if indexes and not isinstance(indexes, list):
            raise TypeError(f"indexes must be a list !")
        if indexes:
            indexes = ','.join(indexes)
            sql = f"{sql.replace(';', '')}interleaved sortkey({indexes});"
        return sql

    def add_columns(self, col_name, col_type):
        sql = f'alter table {self.tablename} add column {col_name} {col_type} default null;'
        return sql


class RedshiftDB(DBCommon, DBFileExec):

    def __init__(self, host, user, password, database, port='5439'):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        super(RedshiftDB, self).__init__()
        self.auto_rules = REDSHIFT_AUTO_RULES
    
    def get_conn(self):
        conn = psycopg2.connect(database=self.database, user=self.user, password=self.password, host=self.host, port=self.port)
        if not conn:
            self.get_conn()
        return conn

    def create(self, tablename, columns, indexes=None):
        # tablename = f"{self.database}.{tablename}"
        sqlcompile = SqlRedshiftCompile(tablename)
        sql_for_create = sqlcompile.create(columns, indexes)
        rows, action, result = self.execute(sql_for_create)
        return rows, action, result

    def add_columns(self, tablename, columns):
        old_columns = self.get_columns(tablename)
        old_columns = set(old_columns)
        new_columns = set(columns)
        # redlogger.info(f'{old_columns}, {new_columns}')

        if old_columns == new_columns:
            redlogger.info(f'【{tablename}】columns not changed !')
        if old_columns - new_columns:
            raise Exception(f"【{tablename}】columns【{old_columns - new_columns}】 not set, should exists !")
        if new_columns - old_columns:
            sqlcompile = SqlRedshiftCompile(tablename)
            add_columns = new_columns - old_columns
            for col_name in add_columns:
                col_type = columns.get(col_name)
                sql = sqlcompile.add_columns(col_name, col_type)
                self.execute(sql)
            redlogger.info(f'【{tablename}】add columns succeed !【{new_columns - old_columns}】')

