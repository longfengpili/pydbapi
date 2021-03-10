# -*- coding: utf-8 -*-
# @Author: chunyang.xu
# @Date:   2021-03-08 14:19:01
# @Last Modified by:   chunyang.xu
# @Last Modified time: 2021-03-10 17:12:02

import os
import pytest
import json
from pydbapi.col import ColumnModel, ColumnsModel
from pydbapi.api import MysqlDB
from pydbapi.api.mysql import SqlMysqlCompile


class TestMysql:

    def setup_method(self, method):
        AdLocal = os.environ.get('ADLOCAL')
        AdLocal = json.loads(AdLocal)
        self.mysqldb = MysqlDB(safe_rule=False, **AdLocal)
        self.tablename = 'test_xu'
        self.id = ColumnModel('id', 'varchar(1024)')
        self.name = ColumnModel('name', 'varchar(1024)')
        self.address = ColumnModel('address', 'varchar(1024)')
        self.birthday = ColumnModel('birthday', 'varchar(1024)')
        self.score = ColumnModel('score', 'varchar(1024)')
        self.columns = ColumnsModel(self.id, self.name, self.address, self.birthday, self.score)

    def teardown_method(self, method):
        pass

    def test_create(self):
        indexes = ['id', 'name']
        rows, action, result = self.mysqldb.create(self.tablename, self.columns, indexes)
        print(f"【rows】: {rows}, 【action】: {action}, 【result】: {result}")

    def test_insert(self):
        values = [[1, 'apple', 'beijing', '2012-01-23', '{"yuwen": 90, "shuxue": 20}'],
                  [2, 'banana', 'shanghai', '2020-02-25 01:00:00', '{"yuwen": 91, "shuxue": 80}'],
                  [3, 'chocolate', 'yunnan', '2020-06-14 23:00:05', '{"yuwen": 90, "shuxue": 90}'],
                  [4, 'pizza', 'taiwan', '2020-05-15 23:08:25', '{"yuwen": 10, "shuxue": 21}'],
                  [5, 'pizza', 'hebei', '2020-08-12 14:05:36', '{"yuwen": 30, "shuxue": 23}']]
        rows, action, result = self.mysqldb.insert(self.tablename, self.columns, values=values)
        print(f"【rows】: {rows}, 【action】: {action}, 【result】: {result}")

    def test_selectsql(self):
        sqlcompile = SqlMysqlCompile(self.tablename)
        sql = sqlcompile.select_base(self.columns, condition='name="apple"')
        print(sql)

    def test_dumpsql(self):
        sqlcompile = SqlMysqlCompile(self.tablename)
        sql = sqlcompile.dumpsql(self.columns, '/tmp/pydbapitest.csv', condition='name="apple"')
        print(sql)

    # @pytest.mark.skip()
    def test_dumpdata(self):
        rows, action, result = self.mysqldb.dumpdata(self.tablename, self.columns, '/tmp/pydbapitest.csv', condition='name="apple"')
        print(f"【rows】: {rows}, 【action】: {action}, 【result】: {result}")

    def test_loaddata(self):
        rows, action, result = self.mysqldb.loaddata(self.tablename, self.columns, '/tmp/pydbapitest.csv')
        print(f"【rows】: {rows}, 【action】: {action}, 【result】: {result}")
