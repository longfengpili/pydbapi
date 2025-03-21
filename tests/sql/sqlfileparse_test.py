# -*- coding: utf-8 -*-
# @Author: longfengpili
# @Date:   2023-06-02 15:27:41
# @Last Modified by:   longfengpili
# @Last Modified time: 2024-11-21 15:15:09
# @github: https://github.com/longfengpili


import os

import pytest
from pydbapi.sql import SqlFileParse, SqlStatements
import pandas as pd

files = os.listdir('./')
print(files)


class TestSqlFileParse:

    def setup_method(self, method):
        self.dirpath = os.path.dirname(os.path.abspath(__file__))
        self.filepath = os.path.join(self.dirpath, 'sql.sql')
        self.withfilepath = os.path.join(self.dirpath, 'withsql.sql')

    def teardown_method(self, method):
        pass

    def test_get_arguments_infile(self):
        filepparser = SqlFileParse(self.filepath)
        content = filepparser.get_content()
        arguments = filepparser.get_arguments_infile(content)
        print(arguments)

    def test_sqls(self):
        filepparser = SqlFileParse(self.filepath)
        arguments, stmts = filepparser.get_filesqls(fpid='12551515, 44546456')
        for purporse, stmt in stmts.items():
            print('='*50)
            print(purporse)
            print(stmt)

    def test_withsql(self):
        filepparser = SqlFileParse(self.withfilepath)
        arguments, stmts = filepparser.get_filesqls(with_test=True, with_snum=2)
        for purporse, stmt in stmts.items():
            print('='*50)
            print(purporse)
            print(stmt.sql)
