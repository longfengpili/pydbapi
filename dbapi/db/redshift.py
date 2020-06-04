# @Author: chunyang.xu
# @Email:  398745129@qq.com
# @Date:   2020-06-03 15:25:44
# @Last Modified time: 2020-06-04 11:14:06
# @github: https://github.com/longfengpili

#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import psycopg2

from .base import DBbase
from dbapi.sql import SqlCompile

import logging
from logging import config

config = config.fileConfig('./dbapi/dblog.conf')
redlog = logging.getLogger('redshift')

class RedshiftDB(DBbase):

    def __init__(self, host=None, user=None, password=None, database=None):
        self.host = host
        self.port = '5439'
        self.user = user
        self.password = password
        self.database = database
    
    def get_conn(self):
        conn = psycopg2.connect(database=self.database, user=self.user, password=self.password, host=self.host, port=self.port)
        if not conn:
            self.get_conn()
        return conn

    def create_table(self, tablename, columns, indexes=None):
        # tablename = f"{self.database}.{tablename}"
        sqlcompile = SqlCompile(tablename)
        sql_for_create = sqlcompile.create_nonindex(columns)
        if indexes and not isinstance(indexes, list):
            raise TypeError(f"indexes must be a list !")

        if indexes:
            indexes = ','.join(indexes)
            sql_for_create = f"{sql_for_create.replace(';', '')}interleaved sortkey({indexes});"

        count, action, result = self.execute(sql_for_create)
        return count, action, result

    def drop_table(self, tablename):
        if '_data_aniland' in tablename:
            sqlcompile = SqlCompile(tablename)
            sql_for_drop = sqlcompile.drop()
            count, action, result = self.execute(sql_for_drop)
            return count, action, result
        else:
            raise Exception(f"【delete】please delete [{tablename}] on workbench!")








