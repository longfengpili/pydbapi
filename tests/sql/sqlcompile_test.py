# @Author: chunyang.xu
# @Email:  398745129@qq.com
# @Date:   2020-11-30 18:43:01
# @Last Modified time: 2020-11-30 19:04:14
# @github: https://github.com/longfengpili

# !/usr/bin/env python3
# -*- coding:utf-8 -*-


from pydbapi.sql import ColumnModel, ColumnsModel, SqlCompile


class TestSqlCompile:

    def setup_method(self, method):
        self.fname = ColumnModel('fname', 'varchar', 'fname', 'fname::varchar', order='2')
        self.lname = ColumnModel('lname', 'varchar', 'lname', 'substr(lname, 2, 10)', order='1')
        self.age_nonfunc = ColumnModel('age', 'integer', 'age', 'age::integer')
        self.age_func = ColumnModel('age', 'integer', 'age', 'age::integer', 'min')
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
