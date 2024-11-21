# -*- coding: utf-8 -*-
# @Author: longfengpili
# @Date:   2024-11-19 14:22:01
# @Last Modified by:   longfengpili
# @Last Modified time: 2024-11-21 15:20:12
# @github: https://github.com/longfengpili


import pytest

from pydbapi.sql.parse import SqlStatement, SqlStatements


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
        self.sql6 = '''
            with currency as (
            select id, currency, currency_id, currency_name, currency_time, rate as exchange_rate, bankofchina_rate
            from mysql."bigdata".t_currency_rate
            where currency = 'JPY'
            ),

            user_daily_info as (
            select date, part_date, role_id, 
            viplevel_max as vip_level_daily, 
            money * 0.0584  as money_lockrate_payment, 
            money * exchange_rate as money_realrate_payment
            from hive.schema.dws_user_daily_di a
            left join currency b
            on date_format(a.date, '%Y-%m') = b.currency_time
            where part_date > $start_date 
            and part_date < $end_date
            and channel between '800001' and '800099'
            ),

            new_user_info as (
            select role_id, open_id, 
            zone_id, channel, install_date, firstpay_date, is_test
            from hive.schema.dws_user_info_di b
            where exists (select 1 from user_daily_info a where a.role_id = b.role_id)
            ),

            user_info as
            (select a.*, 
            b.zone_id, b.channel, b.firstpay_date, b.is_test
            from user_daily_info a
            left join new_user_info b
            on a.role_id = b.role_id
            )

            select *
            from user_info
            ;
        '''
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
        print(sqlstmt)

    def test_params(self):
        sqlstmt = SqlStatement(self.sql5)
        print(sqlstmt.params)

    def test_substitute_params(self):
        sqlstmt = SqlStatement(self.sql5)
        sql = sqlstmt.substitute_params(start_date="'2024-11-25'", name="'tname'")
        print(sqlstmt)
        print(sql.sql)

    def test_get_subqueries(self):
        sqlstmt = SqlStatement(self.sql6)
        print(sqlstmt)
        for idx, subquery in enumerate(sqlstmt.subqueries):
            print(f"{idx}".center(50, '='))
            print(type(subquery))
            print(subquery.get_real_name())

    def test_with_testsql(self):
        sqlstmt = SqlStatement(self.sql6)
        print(sqlstmt)
        combination_sql = sqlstmt.get_with_testsql(2)
        print(combination_sql)
        print(combination_sql.sql)

    @pytest.mark.parametrize("idx", [0, 1, 2, 3, 4])
    def test_stmts_attributes(self, idx: int):
        sql = self.sqls[idx]
        print(f"{idx}".center(50, '='))
        sqlstmts = SqlStatements(sql)
        print(sqlstmts)
