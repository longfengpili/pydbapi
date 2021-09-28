# -*- coding: utf-8 -*-
# @Author: chunyang.xu
# @Date:   2021-03-08 14:19:01
# @Last Modified by:   chunyang.xu
# @Last Modified time: 2021-09-27 18:13:16

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
        AdLocal = json.loads(AdLocal.replace("'", '"'))
        self.mysqldb = MysqlDB(safe_rule=False, **AdLocal)
        self.tablename = 'test_xu'
        self.id = ColumnModel('id', 'varchar(1024)')
        self.name = ColumnModel('name', 'varchar(1024)')
        self.address = ColumnModel('address', 'varchar(1024)')
        self.birthday = ColumnModel('birthday', 'date')
        self.age = ColumnModel('age', 'int')
        self.score = ColumnModel('score', 'varchar(1024)')
        self.fscore = ColumnModel('fscore', 'decimal(8, 2)')
        self.columns = ColumnsModel(self.id, self.name, self.address, self.birthday, self.age, self.score, self.fscore)

    def teardown_method(self, method):
        pass

    def test_create_database(self):
        sql = 'create database test default character set utf8mb4 collate utf8mb4_unicode_ci;'
        print(sql)

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
        values = [[1, 'apple', 'beijing', '2012-01-23', 11, '{"yuwen": 90, "shuxue": 20}', 11.22],
                  [2, 'banana', 'shanghai', '2020-02-25 01:00:00', 13, '{"yuwen": 91, "shuxue": 80}', 13.33],
                  [3, 'chocolate', 'yunnan', '2020-06-14 23:00:05', 10, '{"yuwen": 90, "shuxue": 90}', 10],
                  [4, 'pizza', 'taiwan', '2020-05-15 23:08:25', 3, '{"yuwen": 10, "shuxue": 21}', 3.233],
                  [5, 'pizza', 'hebei', '2020-08-12 14:05:36', 7,  '{"yuwen": 30, "shuxue": 23}', 7.90]]
        rows, action, result = self.mysqldb.insert(self.tablename, self.columns, values=values, chunksize=2, verbose=1)
        print(f"【rows】: {rows}, 【action】: {action}, 【result】: {result}")

    def test_selectsql(self):
        sqlcompile = SqlMysqlCompile(self.tablename)
        sql = sqlcompile.select_base(self.columns, condition='name="apple"')
        print(sql)

    def test_select(self):
        rows, action, result = self.mysqldb.select(self.tablename, self.columns, verbose=1)
        print(f"【rows】: {rows}, 【action】: {action}, 【result】: {result}")

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
