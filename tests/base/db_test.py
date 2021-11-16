# @Author: chunyang.xu
# @Email:  398745129@qq.com
# @Date:   2020-06-03 10:36:14
# @Last Modified time: 2021-11-16 17:06:12
# @github: https://github.com/longfengpili

#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import pytest

from pydbapi.sql import SqlParse
from pydbapi.conf.settings import LOGGING_CONFIG
import logging.config
logging.config.dictConfig(LOGGING_CONFIG)

dblogger = logging.getLogger(__name__)


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
        dblogger.warning(parser.action)
        print(parser.action)

    def test_get_tablename(self):
        parser = SqlParse(self.sql)
        print(parser.tablename)

    def test_get_comment(self):
        parser = SqlParse(self.sql)
        print(parser.comment)
