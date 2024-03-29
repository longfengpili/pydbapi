# -*- coding: utf-8 -*-
# @Author: longfengpili
# @Date:   2023-06-02 15:27:41
# @Last Modified by:   longfengpili
# @Last Modified time: 2023-07-27 15:42:09
# @github: https://github.com/longfengpili


import os

import pytest
from pydbapi.sql import SqlFileParse, SqlParse
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

    def test_sqlparse(self):
        sql = '''
                select 
                * 
                from table1
                ;
                '''
        sparser = SqlParse(sql)
        print(sparser.action, sparser.tablename, sparser.purpose)

        sql = 'create table table2 (id integer);'
        sparser = SqlParse(sql)
        print(sparser.action, sparser.tablename, sparser.purpose)

        sql = 'delete from table2 where 1 = 1;'
        sparser = SqlParse(sql)
        print(sparser.action, sparser.tablename, sparser.purpose)

        sql = 'update table3 set id = 1;'
        sparser = SqlParse(sql)
        print(sparser.action, sparser.tablename, sparser.purpose)

        sql = 'insert into table4 values (1);'
        sparser = SqlParse(sql)
        print(sparser.action, sparser.tablename, sparser.purpose)

        sql = 'select * from table5 a left join table 6 on a.date = b.date;'
        sparser = SqlParse(sql)
        print(sparser.action, sparser.tablename, sparser.purpose)

        sql = 'drop index multiple_index on opm_tw_r_ad_report_reattributed_day;'
        sparser = SqlParse(sql)
        print(sparser.action, sparser.tablename, sparser.purpose)

        sql = 'create index multiple_index on opm_tw_r_ad_report_reattributed_day($report_multiple_index);'
        sparser = SqlParse(sql)
        print(sparser.action, sparser.tablename, sparser.purpose)

        sql = 'show index from opm_tw_r_ad_report_reattributed_day;'
        sparser = SqlParse(sql)
        print(sparser.action, sparser.tablename, sparser.purpose)

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
        content = filepparser.replace_params(date_min='2012-12-12', date_max='2012-12-12', fpid='12551515, 44546456')
        print(content)

    def test_sqls(self):
        filepparser = SqlFileParse(self.filepath)
        arguments, sqls = filepparser.get_filesqls(fpid='12551515, 44546456')
        for sql in sqls:
            print('='*50)
            print(f"{sql}\n{sqls.get(sql)}")

    def test_withsql(self):
        filepparser = SqlFileParse(self.withfilepath)
        arguments, sqls = filepparser.get_filesqls(with_test=True, with_snum=3)
        for sql in sqls:
            print('='*50)
            print(sql)
            print(sqls.get(sql))
