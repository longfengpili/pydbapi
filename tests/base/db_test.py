# @Author: chunyang.xu
# @Email:  398745129@qq.com
# @Date:   2020-06-03 10:36:14
# @Last Modified time: 2020-06-10 11:57:21
# @github: https://github.com/longfengpili

#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import pytest

from pydbapi.sql import SqlParse

@pytest.mark.skip()
class TestDB:

    test = 'test1'

    def setup_method(self, method):
        self.conn = 'test_setup'
        pass

    def teardown_method(self, method):
        pass

    @pytest.mark.skip()
    def test_create(self):
        print(self.conn)

    def test_test(self):
        assert self.test == 'test'


class TestSql:

    sql = '''
        --test comment
        create temp table temp_test as
        select fpid, app_id, sts_data, msg_type, level
        from raw_data_aniland.sdk_data_
        limit 1000;
        '''

    def test_get_action(self):
        parser = SqlParse(self.sql)
        print(parser.action)

    def test_get_tablename(self):
        parser = SqlParse(self.sql)
        print(parser.tablename)

    def test_get_comment(self):
        parser = SqlParse(self.sql)
        print(parser.comment)
