# -*- coding: utf-8 -*-
# @Author: longfengpili
# @Date:   2024-11-19 14:22:01
# @Last Modified by:   longfengpili
# @Last Modified time: 2024-11-19 15:42:18
# @github: https://github.com/longfengpili


import pytest

from pydbapi.sql.parse_new import SqlStatement


class TestSqlStatement:

    def setup_method(self, method):
        self.sql1 = '''
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
        '''
        self.sql2 = 'insert into hive.ht_cn_w.temp_user_daily1;'
        self.sql3 = 'delete from hive.ht_cn_w.temp_user_daily2;'
        self.sql4 = 'drop table if exists hive.ht_cn_w.temp_user_daily3;'
        self.sql5 = "select * from hive.ht_cn_w.temp_user_daily4 where start_date >= $start_date and name = $name;"
        self.sqls = [self.sql1, self.sql2, self.sql3, self.sql4, self.sql5]

    def teardown_method(self, method):
        pass

    def test_tokens(self):
        sqlstmt = SqlStatement(self.sql2)
        print(sqlstmt.tokens)

    def test_sql(self):
        sqlstmt = SqlStatement(self.sql1)
        print(sqlstmt.sql)

    @pytest.mark.parametrize("idx", [0, 1, 2, 3, 4])
    def test_attributes(self, idx: int):
        sql = self.sqls[idx]
        print(f"{idx}".center(50, '='))
        sqlstmt = SqlStatement(sql)
        print(f"{sqlstmt.comment=}\n{sqlstmt.action=}\n{sqlstmt.tablename=}")

    def test_params(self):
        sqlstmt = SqlStatement(self.sql5)
        print(sqlstmt.params)

    def test_substitute_params(self):
        sqlstmt = SqlStatement(self.sql5)
        sql = sqlstmt.substitute_params(start_date="'2024-11-25'", name="'tname'")
        print(f"{sqlstmt.comment=}\n{sqlstmt.action=}\n{sqlstmt.tablename=}")
        print(sql.sql)
