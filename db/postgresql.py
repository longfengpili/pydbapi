# @Author: chunyang.xu
# @Email:  398745129@qq.com
# @Date:   2020-06-03 15:25:44
# @Last Modified time: 2020-06-03 17:22:08
# @github: https://github.com/longfengpili

#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import psycopg2

from .base import DBbase

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







