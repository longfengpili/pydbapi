# @Author: chunyang.xu
# @Email:  398745129@qq.com
# @Date:   2020-06-03 15:25:44
# @Last Modified time: 2020-06-04 11:59:15
# @github: https://github.com/longfengpili

#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import psycopg2

from .base import DBCommon
from dbapi.sql import SqlCompile


import logging
from logging import config

config = config.fileConfig('./dbapi/dblog.conf')
redlog = logging.getLogger('redshift')

class RedshiftDB(DBCommon):

    def __init__(self, host=None, user=None, password=None, database=None):
        self.host = host
        self.port = '5439'
        self.user = user
        self.password = password
        self.database = database
        super(RedshiftDB, self).__init__()
    
    def get_conn(self):
        conn = psycopg2.connect(database=self.database, user=self.user, password=self.password, host=self.host, port=self.port)
        if not conn:
            self.get_conn()
        return conn

    def create(self, tablename, columns, indexes=None):
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

    








