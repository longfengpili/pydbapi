# -*- coding: utf-8 -*-
# @Author: chunyang.xu
# @Date:   2023-06-02 15:27:41
# @Last Modified by:   chunyang.xu
# @Last Modified time: 2023-07-26 18:18:37
# @github: https://github.com/longfengpili

from pydbapi.col import ColumnModel, ColumnsModel
from pydbapi.sql import SqlCompile


class TestSqlCompile:

    def setup_method(self, method):
        self.fname = ColumnModel('name', 'varchar', 'fname + lname', order=2)
        self.lname = ColumnModel('lname', 'varchar', 'substr(lname, 2, 10)', order=1)
        self.age_nonfunc = ColumnModel('age', 'integer', 'age')
        self.age_func = ColumnModel('age', 'integer', 'age', 'min')
        self.columns1 = ColumnsModel(self.fname, self.lname, self.age_nonfunc)
        self.columns2 = ColumnsModel(self.lname, self.fname, self.age_func)
        self.tablename = 'raw_data.test'
        self.condition = 'where 1 = 1'
        self.values = [['xu', 'small', 12], ['li', 'large', 14]]

    def teardown_method(self, method):
        pass

    def test_select_base(self):
        sqlcompile = SqlCompile(self.tablename)
        sql = sqlcompile.select_base(self.columns1)
        print(sql)
        sql = sqlcompile.select_base(self.columns2, self.condition)
        print(sql)

    def test_create_nonindex(self):
        sqlcompile = SqlCompile(self.tablename)
        sql = sqlcompile.create_nonindex(self.columns1)
        print(sql)
        sql = sqlcompile.create_nonindex(self.columns2)
        print(sql)

    def test_insert_by_value(self):
        sqlcompile = SqlCompile(self.tablename)
        sql = sqlcompile._insert_by_value(self.columns1, self.values)
        print(sql)
        sql = sqlcompile._insert_by_value(self.columns2, self.values)
        print(sql)

    def test_insert(self):
        sqlcompile = SqlCompile(self.tablename)
        sql = sqlcompile.insert(self.columns1, inserttype='value', values=self.values)
        print(sql)
        sql = sqlcompile.insert(self.columns2, inserttype='select', fromtable='raw_data.fromtest')
        print(sql)
