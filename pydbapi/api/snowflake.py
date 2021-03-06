# @Author: chunyang.xu
# @Email:  398745129@qq.com
# @Date:   2020-10-22 16:12:47
# @Last Modified time: 2020-12-02 14:28:27
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
sflogger = logging.getLogger('snowflake')


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
            sql = f"{sql.replace(';', '')}\ncluster by ({indexes});"
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
