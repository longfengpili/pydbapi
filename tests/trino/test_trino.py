# -*- coding: utf-8 -*-
# @Author: longfengpili
# @Date:   2022-11-14 14:25:01
# @Last Modified by:   longfengpili
# @Last Modified time: 2022-11-16 11:14:15


import sys
sys.path.append(r'D:\GoogleDrive\ourpalm\workspace\2022.11.14-trino-python\trino-example')

import os
import pytest
import json
from pydbapi.col import ColumnModel, ColumnsModel
from pydbapi.api import TrinoDB
from pydbapi.api.trino import SqlTrinoCompile

# 如果需要日期，请打开
# from pydbapi.conf.settings import LOGGING_CONFIG
# import logging.config
# logging.config.dictConfig(LOGGING_CONFIG)


class TestTrino:

    def setup_method(self, method):
        GAME = os.environ.get('NEWGAME').lower()
        self.game = json.loads(GAME.replace("'", '"'))
        self.trinodb = TrinoDB(**self.game, safe_rule=False)
        self.tablename = 'report_20000073_11.test_friuts'
        self.id = ColumnModel('id', 'integer')
        self.name = ColumnModel('name', 'varchar(1024)')
        self.address = ColumnModel('address', 'varchar(1024)')
        self.birthday = ColumnModel('birthday', 'varchar(1024)')
        self.score = ColumnModel('score', 'varchar(1024)')
        self.columns = ColumnsModel(self.id, self.name, self.address, self.birthday, self.score)

    def teardown_method(self, method):
        pass

    def test_get_instance(self):
        print(self.trinodb)
        print(dir(self.trinodb))

    def test_create_by_sql1(self):
        sql = '''
        create table report_20000073_11.test_xu
        (time varchar,
        adid varchar,
        dt varchar)
        with (partitioned_by = ARRAY['dt']);
        '''
        rows, action, result = self.trinodb.execute(sql, verbose=1)
        print(f"【rows】: {rows}, 【action】: {action}, 【result】: {result}")

    def test_create_by_sql2(self):
        sql = '''
        create table if not exists report_20000073_11.test_xu as 
        with test as
        (select * 
        from logs_thirdparty.adjust_callback 
        limit 10),

        test1 as
        (select time, adid, substring(time, 1, 10) as dt
        from test
        )

        select * from test1
        ;
        '''
        rows, action, result = self.trinodb.execute(sql, verbose=1)
        print(f"【rows】: {rows}, 【action】: {action}, 【result】: {result}")

    def test_insert_by_sql(self):
        sql = '''
            delete from report_20000073_11.test_xu;
            with test as
            (select * 
            from logs_thirdparty.adjust_callback 
            limit 10),

            test1 as
            (select time, adid, substring(time, 1, 10) as dt
            from test
            )

            select * from test1
            ;
            insert into report_20000073_11.test_xu
            with test as
            (select * 
            from logs_thirdparty.adjust_callback 
            limit 10),

            test1 as
            (select time, adid, substring(time, 1, 10) as dt
            from test
            )

            select * from test1
            ;
        '''
        rows, action, result = self.trinodb.execute(sql, verbose=1)
        print(f"【rows】: {rows}, 【action】: {action}, 【result】: {result}")

    def test_drop_by_sql(self):
        sql = '''
        drop table report_20000073_11.test_xu
        '''
        rows, action, result = self.trinodb.execute(sql)
        print(f"【rows】: {rows}, 【action】: {action}, 【result】: {result}")

    def test_select_by_sql(self):
        sql = '''
        with test as
        (select * 
        from logs_thirdparty.adjust_callback 
        limit 10),

        test1 as
        (select time, adid, substring(time, 1, 10) as dt
        from test
        )

        select * from test1
        '''
        rows, action, result = self.trinodb.execute(sql, verbose=1)
        print(f"【rows】: {rows}, 【action】: {action}, 【result】: {result}")

    def test_create(self):
        rows, action, result = self.trinodb.create(self.tablename, self.columns, partition='birthday')
        print(f"【rows】: {rows}, 【action】: {action}, 【result】: {result}")

    def test_drop(self):
        rows, action, result = self.trinodb.drop(self.tablename)
        print(f"【rows】: {rows}, 【action】: {action}, 【result】: {result}")

    def test_insertsql(self):
        values = [[1, 'apple', 'beijing', '2012-01-23', '{"yuwen": 90, "shuxue": 20}'],
                  [2, 'banana', 'shanghai', '2020-02-25 01:00:00', '{"yuwen": 91, "shuxue": 80}'],
                  [3, 'chocolate', 'yunnan', '2020-06-14 23:00:05', '{"yuwen": 90, "shuxue": 90}'],
                  [4, 'pizza', 'taiwan', '2020-05-15 23:08:25', '{"yuwen": 10, "shuxue": 21}'],
                  [5, 'pizza', 'hebei', '2020-08-12 14:05:36', '{"yuwen": 30, "shuxue": 23}']]
        sqlcompile = SqlTrinoCompile(self.tablename)
        sql = sqlcompile.insert(self.columns, inserttype='value', values=values, chunksize=1000)
        print(sql)

    def test_insert(self):
        values = [[1, 'apple', 'beijing', '2012-01-23', '{"yuwen": 90, "shuxue": 20}'],
                  [2, 'banana', 'shanghai', '2020-02-25 01:00:00', '{"yuwen": 91, "shuxue": 80}'],
                  [3, 'chocolate', 'yunnan', '2020-06-14 23:00:05', '{"yuwen": 90, "shuxue": 90}'],
                  [4, 'pizza', 'taiwan', '2020-05-15 23:08:25', '{"yuwen": 10, "shuxue": 21}'],
                  [5, 'pizza', 'hebei', '2020-08-12 14:05:36', '{"yuwen": 30, "shuxue": 23}']]

        rows, action, result = self.trinodb.create(self.tablename, self.columns, partition='birthday')
        rows, action, result = self.trinodb.insert(self.tablename, self.columns, values=values, chunksize=1, verbose=1)
        print(f"【rows】: {rows}, 【action】: {action}, 【result】: {result}")

    def test_selectsql(self):
        sqlcompile = SqlTrinoCompile(self.tablename)
        sql = sqlcompile.select_base(self.columns, condition="name='pizza'")
        print(sql)

    def test_select(self):
        rows, action, result = self.trinodb.select(self.tablename, self.columns, condition="name='pizza'", verbose=1)
        print(f"【rows】: {rows}, 【action】: {action}, 【result】: {result}")
