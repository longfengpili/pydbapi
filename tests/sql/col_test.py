# @Author: chunyang.xu
# @Email:  398745129@qq.com
# @Date:   2020-11-30 18:16:59
# @Last Modified time: 2021-02-01 19:24:53
# @github: https://github.com/longfengpili

# !/usr/bin/env python3
# -*- coding:utf-8 -*-


from pydbapi.col import ColumnModel, ColumnsModel


class TestSqlFileParse:

    def setup_method(self, method):
        self.name = ColumnModel('name', 'varchar', 'fname + lname', order=1)
        self.fname = ColumnModel('fname', 'varchar', 'fname', order=3)
        self.lname = ColumnModel('lname', 'varchar', 'substr(lname, 2, 10)', order=2)
        self.age_nonfunc = ColumnModel('age', 'integer', 'age')
        self.age_func = ColumnModel('age', 'integer', 'age', 'min')

    def teardown_method(self, method):
        pass

    def test_column(self):
        print(self.fname)
        print(self.lname)
        print(self.age_nonfunc)
        print(self.age_func)

    def test_column_nonfunc(self):
        columns = ColumnsModel(self.name, self.fname, self.lname, self.age_nonfunc)
        print("new_cols:".center(50, '=') + f"\n{columns.new_cols}")
        print("create_cols:".center(50, '=') + f"\n{columns.create_cols}")
        print("nonfunc_cols:".center(50, '=') + f"\n{columns.nonfunc_cols}")
        print("func_cols:".center(50, '=') + f"\n{columns.func_cols}")
        print("order_cols:".center(50, '=') + f"\n{columns.order_cols}")

    def test_column_func(self):
        columns = ColumnsModel(self.name, self.fname, self.lname, self.age_func)
        print("new_cols:".center(50, '=') + f"\n{columns.new_cols}")
        print("create_cols:".center(50, '=') + f"\n{columns.create_cols}")
        print("nonfunc_cols:".center(50, '=') + f"\n{columns.nonfunc_cols}")
        print("func_cols:".center(50, '=') + f"\n{columns.func_cols}")
        print("order_cols:".center(50, '=') + f"\n{columns.order_cols}")
