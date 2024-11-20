# -*- coding: utf-8 -*-
# @Author: longfengpili
# @Date:   2024-11-19 16:04:33
# @Last Modified by:   longfengpili
# @Last Modified time: 2024-11-19 16:20:13
# @github: https://github.com/longfengpili

import pytest

from pydbapi.sql.parse_new import SqlParse


class TestSqlStatement:

    def setup_method(self, method):
        self.sql = '''
            -- porpose test
            -- comment test
            create table if not exists hive.ht_cn_w.temp_user_daily as
            with test as
            (select *
            from hive.ht_cn_w.dws_user_daily_di
            where part_date >= $start_date
            and part_date <= $end_date
            limit 10)

            select *
            from test
            ;

            select * from hive.ht_cn_w.temp_user_daily;
        '''

    def teardown_method(self, method):
        pass

    def test_statments(self):
        sp = SqlParse(self.sql)
        print(sp.statements)
