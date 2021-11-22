# -*- coding: utf-8 -*-
# @Author: chunyang.xu
# @Date:   2021-03-08 14:19:01
# @Last Modified by:   chunyang.xu
# @Last Modified time: 2021-11-20 15:35:10

import os
import pytest
import json
from pydbapi.col import ColumnModel, ColumnsModel
from pydbapi.api import MysqlDB
from pydbapi.api.mysql import SqlMysqlCompile

from pydbapi.conf.settings import LOGGING_CONFIG
import logging.config
logging.config.dictConfig(LOGGING_CONFIG)


class TestMysql:

    def setup_method(self, method):
        AdLocal = os.environ.get('ADLOCAL').lower()
        self.AdLocal = json.loads(AdLocal.replace("'", '"'))
        self.mysqldb = MysqlDB(safe_rule=False, **self.AdLocal)
        self.tablename = 'test_xu'
        self.id = ColumnModel('id', 'varchar(1024)')
        self.name = ColumnModel('name', 'varchar(1024)')
        self.address = ColumnModel('address', 'varchar(1024)')
        self.birthday = ColumnModel('birthday', 'date')
        self.score = ColumnModel('score', 'varchar(1024)')
        self.columns = ColumnsModel(self.id, self.name, self.address, self.birthday, self.score)

    def teardown_method(self, method):
        pass

    def test_get_instance(self):
        mysql1 = MysqlDB.get_instance(safe_rule=False, **self.AdLocal)
        print(mysql1)
        # mysql2 = MysqlDB.get_instance(safe_rule=False, **self.AdLocal)
        mysql3 = MysqlDB(safe_rule=False, **self.AdLocal)
        # mysql4 = MysqlDB(safe_rule=False, **self.AdLocal)
        # print(mysql3, mysql4)
        # for i in dir(mysql4):
        #     result = eval(f"mysql4.{i}")
        #     print(f"【{i}】: {result}")

    def test_drop(self):
        rows, action, result = self.mysqldb.drop(self.tablename)
        print(f"【rows】: {rows}, 【action】: {action}, 【result】: {result}")

    def test_createsql(self):
        indexes = ['id', 'name']
        sqlcompile = SqlMysqlCompile(self.tablename)
        sql = sqlcompile.create(self.columns, indexes, partition='birthday')
        print(sql)

    def test_create(self):
        indexes = ['id', 'name']
        rows, action, result = self.mysqldb.create(self.tablename, self.columns, indexes, partition='birthday')
        print(f"【rows】: {rows}, 【action】: {action}, 【result】: {result}")

    def test_insert(self):
        values = [[1, 'apple', 'beijing', '2012-01-23', '{"yuwen": 90, "shuxue": 20}'],
                  [2, 'banana', 'shanghai', '2020-02-25 01:00:00', '{"yuwen": 91, "shuxue": 80}'],
                  [3, 'chocolate', 'yunnan', '2020-06-14 23:00:05', '{"yuwen": 90, "shuxue": 90}'],
                  [4, 'pizza', 'taiwan', '2020-05-15 23:08:25', '{"yuwen": 10, "shuxue": 21}'],
                  [5, 'pizza', 'hebei', '2020-08-12 14:05:36', '{"yuwen": 30, "shuxue": 23}']]
        rows, action, result = self.mysqldb.insert(self.tablename, self.columns, values=values, chunksize=2, verbose=1)
        print(f"【rows】: {rows}, 【action】: {action}, 【result】: {result}")

    def test_selectsql(self):
        sqlcompile = SqlMysqlCompile(self.tablename)
        sql = sqlcompile.select_base(self.columns, condition='name="apple"')
        print(sql)

    def test_dumpsql(self):
        sqlcompile = SqlMysqlCompile(self.tablename)
        sql = sqlcompile.dumpsql(self.columns, '/tmp/pydbapitest.csv', condition='name="apple"')
        print(sql)

    @pytest.mark.skip()
    def test_dumpdata(self):
        rows, action, result = self.mysqldb.dumpdata(self.tablename, self.columns, '/tmp/pydbapitest.csv', condition='name="apple"')
        print(f"【rows】: {rows}, 【action】: {action}, 【result】: {result}")

    @pytest.mark.skip()
    def test_loaddata(self):
        rows, action, result = self.mysqldb.loaddata(self.tablename, self.columns, '/tmp/pydbapitest.csv')
        print(f"【rows】: {rows}, 【action】: {action}, 【result】: {result}")
