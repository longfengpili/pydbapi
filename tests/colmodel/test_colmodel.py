# -*- coding: utf-8 -*-
# @Author: chunyang.xu
# @Date:   2021-09-24 12:10:12
# @Last Modified by:   longfengpili
# @Last Modified time: 2023-06-08 10:28:51


from pydbapi.col import ColumnModel, ColumnsModel


class TestColModel:

    def setup_method(self, method):
        self.id = ColumnModel('id', 'varchar(1024)')
        self.name = ColumnModel('name', 'varchar(1024)')
        self.address = ColumnModel('address', 'varchar(1024)', desc='地址')
        self.birthday = ColumnModel('birthday', 'date')
        self.score = ColumnModel('score', 'varchar(1024)')
        self.columns = ColumnsModel(self.id, self.name, self.address, self.birthday, self.score)

    def teardown_method(self, method):
        pass

    def test_column_getter(self):
        colname = 'score'
        col = self.columns.index(0)
        print(col)
        col = self.columns.get_column_by_name(colname)
        print(col)
        col = self.columns[0]
        print(col)

    def test_column_contain(self):
        colname = 'score'
        print(colname in self.columns)

        colname = 'score1'
        print(colname in self.columns)

    def test_columns_iterable(self):
        print(self.columns)
        for col in self.columns:
            print(col)
