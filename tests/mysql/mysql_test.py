# -*- coding: utf-8 -*-
# @Author: longfengpili
# @Date:   2023-06-02 15:27:41
# @Last Modified by:   longfengpili
# @Last Modified time: 2024-11-13 10:29:09
# @github: https://github.com/longfengpili


import os
import pytest
import json
from pydbapi.model import ColumnModel, ColumnsModel
from pydbapi.api import MysqlDB
from pydbapi.api.mysql import SqlMysqlCompile

# 如果需要日期，请打开
# from pydbapi.conf.logconf import LOGGING_CONFIG
# import logging.config
# logging.config.dictConfig(LOGGING_CONFIG)


class TestMysql:

    def setup_method(self, method):
        self.LocalDB = {'host': 'localhost', 'port': 3306, 'user': 'longfengpili', 'password': '123456abc', 'database': 'test'}
        self.mysqldb = MysqlDB(safe_rule=False, **self.LocalDB)
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
        # mysql1 = MysqlDB.get_instance(safe_rule=False, **self.LocalDB)
        # print(mysql1)
        # mysql2 = MysqlDB.get_instance(safe_rule=False, **self.LocalDB)
        mysql3 = MysqlDB(safe_rule=False, **self.LocalDB)
        # mysql4 = MysqlDB(safe_rule=False, **self.LocalDB)
        # print(mysql3, mysql4)
        # for i in dir(mysql4):
        #     result = eval(f"mysql4.{i}")
        #     print(f"【{i}】: {result}")

    def test_create_for_drop(self):
        indexes = ['id', 'name']
        cursor, action, result = self.mysqldb.create(self.tablename, self.columns, indexes, partition='birthday')
        print(f"【cur】: {cursor}, 【action】: {action}, 【result】: {result}")

    def test_drop(self):
        cursor, action, result = self.mysqldb.drop(self.tablename)
        print(f"【cur】: {cursor}, 【action】: {action}, 【result】: {result}")

    def test_createsql(self):
        indexes = ['id', 'name']
        sqlcompile = SqlMysqlCompile(self.tablename)
        sql = sqlcompile.create(self.columns, indexes, partition='birthday')
        print(sql)

    def test_create(self):
        indexes = ['id', 'name']
        cursor, action, result = self.mysqldb.create(self.tablename, self.columns, indexes, partition='birthday')
        print(f"【cur】: {cursor}, 【action】: {action}, 【result】: {result}")

    def test_cols(self):
        result = self.mysqldb.get_columns(self.tablename)
        print(result)

    def test_insert(self):
        values = [[1, 'apple', 'beijing', '2012-01-23', '{"yuwen": 90, "shuxue": 20}'],
                  [2, 'banana', 'shanghai', '2020-02-25 01:00:00', '{"yuwen": 91, "shuxue": 80}'],
                  [3, 'chocolate', 'yunnan', '2020-06-14 23:00:05', '{"yuwen": 90, "shuxue": 90}'],
                  [4, 'pizza', 'taiwan', '2020-05-15 23:08:25', '{"yuwen": 10, "shuxue": 21}'],
                  [5, 'pizza', 'hebei', '2020-08-12 14:05:36', '{"yuwen": 30, "shuxue": 23}']]
        cursor, action, result = self.mysqldb.insert(self.tablename, self.columns, values=values, chunksize=2, verbose=1)
        print(f"【cur】: {cursor}, 【action】: {action}, 【result】: {result}")

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
        cursor, action, result = self.mysqldb.dumpdata(self.tablename, self.columns, '/tmp/pydbapitest.csv', condition='name="apple"')
        print(f"【cur】: {cursor}, 【action】: {action}, 【result】: {result}")

    @pytest.mark.skip()
    def test_loaddata(self):
        cursor, action, result = self.mysqldb.loaddata(self.tablename, self.columns, '/tmp/pydbapitest.csv')
        print(f"【cur】: {cursor}, 【action】: {action}, 【result】: {result}")

    def test_select_by_sql(self):
        sql = f'''
            -- test1
            with test as
            (select * 
            from {self.tablename} 
            limit 10),

            test1 as
            (select birthday as time, name as adid, substring(birthday, 1, 10) as dt
            from test
            )

            select * from test1
            ;

            -- test2
            with test as
            (select * 
            from {self.tablename} 
            limit 10),

            test1 as
            (select birthday as time, name as adid, substring(birthday, 1, 10) as dt
            from test
            )

            select * from test1
            ;

            -- test3
            with test as
            (select * 
            from {self.tablename} 
            limit 10),

            test1 as
            (select birthday as time, name as adid, substring(birthday, 1, 10) as dt
            from test
            )

            select * from test1
            ;
        '''
        # print(sql)
        cursor, action, result = self.mysqldb.execute(sql, verbose=3)
        print(f"【cur】: {cursor}, 【action】: {action}, 【result】: {result}")

    def test_alter_table(self):
        self.mysqldb.alter_tablecol(self.tablename, colname='id', newname='id_new', newtype='real')
        self.mysqldb.alter_tablecol(self.tablename, colname='id_new', newname='id')
