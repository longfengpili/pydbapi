# -*- coding: utf-8 -*-
# @Author: longfengpili
# @Date:   2023-06-05 17:44:29
# @Last Modified by:   longfengpili
# @Last Modified time: 2023-06-05 19:01:48


import os
import pytest
import json
from pydbapi.col import ColumnModel, ColumnsModel
from pydbapi.api import TaDB, SqlTaCompile

# # 如果需要日期，请打开
# from pydbapi.conf.settings import LOGGING_CONFIG
# import logging.config
# logging.config.dictConfig(LOGGING_CONFIG)


class TestTa:

    def setup_method(self, method):
        self.url = ''
        self.token = ''
        self.tadb = TaDB(url=self.url, token=self.token)
        self.tablename = 'v_event_19'
        self.uid = ColumnModel('"#user_id"', 'varchar(1024)')
        self.level = ColumnModel('level', 'integer')
        self.openid = ColumnModel('openid', 'varchar(1024)')
        self.date = ColumnModel('date', 'date')
        self.money = ColumnModel('money', 'integer(1024)')
        self.columns = ColumnsModel(self.uid, self.level, self.openid, self.date, self.money)

    def teardown_method(self, method):
        pass

    def test_get_instance(self):
        Ta1 = TaDB(url=self.url, token=self.token)
        Ta2 = TaDB(url=self.url, token=self.token)
        ta3 = TaDB.get_instance(url=self.url, token=self.token)
        ta4 = TaDB.get_instance(url=self.url, token=self.token)
        print(Ta1, Ta2, ta3, ta4)

    def test_drop(self):
        rows, action, result = self.tadb.drop()
        print(f"【rows】: {rows}, 【action】: {action}, 【result】: {result}")

    def test_selectsql(self):
        sqlcompile = SqlTaCompile(self.tablename)
        condition = '''
            "$part_event"='Payment' AND "$part_date"='2023-06-04'
        '''
        sql = sqlcompile.select_base(self.columns, condition=condition)
        print(sql)

    def test_select(self):
        condition = '''
            "$part_event"='Payment' AND "$part_date"='2023-06-04' limit 10
        '''
        rows, action, result = self.tadb.select(self.tablename, self.columns, condition=condition)
        print(rows, action, result)

    def test_execute(self):
        sql = '''
            select "#user_id", level, openid, date, money
            from v_event_19
            where "$part_event"='Payment' AND "$part_date"='2023-06-04'
            limit 10
            ;
        '''
        rows, action, result = self.tadb.execute(sql)
        print(rows, action, result)

    def test_execfile(self):
        dirpath = os.path.dirname(os.path.abspath(__file__))
        sqlfile = os.path.join(dirpath, 'ta.sql')
        results = self.tadb.file_exec(sqlfile)
        print(results)
