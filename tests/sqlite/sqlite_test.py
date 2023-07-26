# -*- coding: utf-8 -*-
# @Author: chunyang.xu
# @Date:   2023-06-02 15:27:41
# @Last Modified by:   chunyang.xu
# @Last Modified time: 2023-07-26 18:18:59
# @github: https://github.com/longfengpili

import os
import pytest
from pydbapi.col import ColumnModel, ColumnsModel
from pydbapi.api import SqliteDB
import pandas as pd

# 是否打印sql
# import logging
# dblogger = logging.getLogger('pydbapi.db.base')
# dblogger.setLevel(logging.DEBUG)


class TestSqlite:

    def setup_method(self, method):
        self.sqlite = SqliteDB()
        self.tablename = 'test_xu'
        self.id = ColumnModel('id', 'varchar')
        self.name = ColumnModel('name', 'varchar')
        self.address = ColumnModel('address', 'varchar(1024)')
        self.birthday = ColumnModel('birthday', 'varchar')
        self.score = ColumnModel('score', 'varchar(1024)')
        self.columns = ColumnsModel(self.id, self.name, self.address, self.birthday, self.score)

    def teardown_method(self, method):
        pass

    # @pytest.mark.skip()
    def test_get_conn(self):
        conn1 = self.sqlite.get_conn()
        conn2 = self.sqlite.get_conn()
        print(conn1, conn2)

    @pytest.mark.skip()
    def test_execute(self):
        sql = '''
        --test
        select json_extract('{"a":2, "c":[4,5,{"f":7}]}', '$.c[0]');
        '''
        rows, action, result = self.sqlite.execute(sql)
        print(f"【rows】: {rows}, 【action】: {action}, 【result】: {result}")

    def test_create(self):
        indexes = ['id', 'name']
        rows, action, result = self.sqlite.create(self.tablename, self.columns, indexes)
        print(f"【rows】: {rows}, 【action】: {action}, 【result】: {result}")

    def test_drop(self):
        rows, action, result = self.sqlite.drop(self.tablename)
        print(f"【rows】: {rows}, 【action】: {action}, 【result】: {result}")

    def test_create1(self):
        indexes = ['id', 'name']
        rows, action, result = self.sqlite.create(self.tablename, self.columns, indexes)
        print(f"【rows】: {rows}, 【action】: {action}, 【result】: {result}")

    def test_insert(self):
        values = [[1, 'apple', 'beijing', '2012-01-23', '{"yuwen": 90, "shuxue": 20}'],
                  [2, 'banana', 'shanghai', '2020-02-25 01:00:00', '{"yuwen": 91, "shuxue": 80}'],
                  [3, 'chocolate', 'yunnan', '2020-06-14 23:00:05', '{"yuwen": 90, "shuxue": 90}'],
                  [4, 'pizza', 'taiwan', '2020-05-15 23:08:25', '{"yuwen": 10, "shuxue": 21}'],
                  [5, 'pizza', 'hebei', '2020-08-12 14:05:36', '{"yuwen": 30, "shuxue": 23}']]
        values *= 20
        rows, action, result = self.sqlite.insert(self.tablename, self.columns, values=values, verbose=1, chunksize=10)
        print(f"【rows】: {rows}, 【action】: {action}, 【result】: {result}")

    def test_select(self):
        yuwen = ColumnModel('yuwen', sqlexpr="score", order=1)
        columns = ColumnsModel(self.id, self.name, yuwen)
        rows, action, result = self.sqlite.select(self.tablename, columns)
        print(f"【rows】: {rows}, 【action】: {action}, 【result】: \n{pd.DataFrame(result[1:], columns=result[0])}")

    def test_getcol(self):
        columns = self.sqlite.get_columns(self.tablename)
        print(f" 【columns】: {columns}")

    def test_addcol(self):
        author = ColumnModel('author')
        author1 = ColumnModel('author1')
        columns = ColumnsModel(self.id, self.name, self.address, self.birthday, self.score, author, author1)
        self.sqlite.add_columns(self.tablename, columns)

    def test_execfile(self):
        dirpath = os.path.dirname(os.path.abspath(__file__))
        filepath = os.path.join(dirpath, 'sqlite.sql')
        self.sqlite.file_exec(filepath, name='pizza', ehandling='raises', verbose=0)  # raise的时候才会报错

    def test_get_instance(self):
        # sqlite1 = SqliteDB.get_instance()
        # sqlite2 = SqliteDB.get_instance()
        sqlite3 = SqliteDB()
        sqlite4 = SqliteDB()
        print(sqlite3, sqlite4)
        print(sqlite4.database)
        # print(sqlite3, sqlite4)
        # for i in dir(sqlite4):
        #     result = eval(f"sqlite4.{i}")
        #     print(f"【{i}】: {result}")

    def test_verbose(self):
        sqlite = SqliteDB(database=None)
        sql = 'select * from test_xu;'
        sqlite.execute(sql, verbose=0)
        # sqlite.execute(sql, verbose=1)
        sqlite.execute(sql, verbose=2)
        # sqlite.execute(sql, verbose=3)
