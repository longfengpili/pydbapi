# @Author: chunyang.xu
# @Email:  398745129@qq.com
# @Date:   2020-06-04 17:45:04
# @Last Modified time: 2020-07-01 16:43:00
# @github: https://github.com/longfengpili

#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import os

import pytest
from pydbapi.sql import SqlFileParse
import pandas as pd

files = os.listdir('./')
print(files)


class TestSqlFileParse:

    def setup_method(self, method):
        self.dirpath = os.path.dirname(os.path.abspath(__file__))
        self.filepath = os.path.join(self.dirpath,'sql.sql')

    def teardown_method(self, method):
        pass

    # @pytest.mark.skip()
    def test_params(self):
        filepparser = SqlFileParse(self.filepath)
        print(filepparser.parameters)

    def test_arguments(self):
        filepparser = SqlFileParse(self.filepath)
        print(filepparser.arguments)

    # @pytest.mark.skip()
    def test_repalceparams(self):
        filepparser = SqlFileParse(self.filepath)
        content = filepparser.replace_params(date_min='2012-12-12', date_max='2012-12-12', fpid= '12551515, 44546456')
        print(content)

    def test_sqls(self):
        filepparser = SqlFileParse(self.filepath)
        sqls = filepparser.get_sqls(date_min='2012-12-12', date_max='2012-12-12', fpid= '12551515, 44546456')
        for sql in sqls:
            print('='*50)
            print(f"{sql}\n{sqls.get(sql)}")


