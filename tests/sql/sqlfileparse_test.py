# -*- coding: utf-8 -*-
# @Author: longfengpili
# @Date:   2023-06-02 15:27:41
# @Last Modified by:   longfengpili
# @Last Modified time: 2024-09-27 18:20:49
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

    def test_get_arguments_infile(self):
        filepparser = SqlFileParse(self.filepath)
        content = filepparser.get_content()
        arguments = filepparser.get_arguments_infile(content)
        print(arguments)

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
