# -*- coding: utf-8 -*-
# @Author: chunyang.xu
# @Date:   2021-09-24 12:10:12
# @Last Modified by:   chunyang.xu
# @Last Modified time: 2021-12-30 19:46:13


from pydbapi.col import ColumnModel, ColumnsModel


class TestColModel:

    def setup_method(self, method):
        self.id = ColumnModel('id', 'varchar(1024)')
        self.name = ColumnModel('name', 'varchar(1024)')
        self.address = ColumnModel('address', 'varchar(1024)')
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
