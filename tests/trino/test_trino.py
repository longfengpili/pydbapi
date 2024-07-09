# -*- coding: utf-8 -*-
# @Author: longfengpili
# @Date:   2023-06-02 15:27:41
# @Last Modified by:   longfengpili
# @Last Modified time: 2024-07-09 13:56:44
# @github: https://github.com/longfengpili


import pytest
from pydbapi.model import ColumnModel, ColumnsModel
from pydbapi.api import TrinoDB
from pydbapi.api.trino import SqlTrinoCompile

from ..variables import TRINO_HOST, TRINO_USER, TRINO_PASSWORD, TRINO_DATABASE

# 如果需要日期，请打开
# from pydbapi.conf.logconf import LOGGING_CONFIG
# import logging.config
# logging.config.dictConfig(LOGGING_CONFIG)


# @pytest.mark.skip(reason='跳过')
class TestTrino:

    def setup_method(self, method):
        self.trinodb = TrinoDB(TRINO_HOST, TRINO_USER, TRINO_PASSWORD, TRINO_DATABASE, safe_rule=False)
        self.tablename = 'warship_jp_w.test_friut_xu'
        self.id = ColumnModel('id', 'varchar')
        self.name = ColumnModel('name', 'varchar(1024)')
        self.address = ColumnModel('address', 'varchar(1024)')
        self.birthday = ColumnModel('birthday', 'varchar(1024)')
        self.score = ColumnModel('score', 'varchar(1024)')
        self.columns = ColumnsModel(self.id, self.name, self.address, self.birthday, self.score)

    def teardown_method(self, method):
        pass

    def test_get_instance(self):
        print(self.trinodb)
        print(dir(self.trinodb))

    def test_create_for_drop(self):
        tablename = f"{self.tablename}_for_drop"
        rows, action, result = self.trinodb.create(tablename, self.columns, partition='birthday')
        print(f"【rows】: {rows}, 【action】: {action}, 【result】: {result}")

    def test_drop(self):
        tablename = f"{self.tablename}_for_drop"
        rows, action, result = self.trinodb.drop(tablename)
        print(f"【rows】: {rows}, 【action】: {action}, 【result】: {result}")

    def test_get_columns(self):
        try:
            columns = self.trinodb.get_columns(self.tablename)
            print(columns)
            self.trinodb.drop(self.tablename)
        except Exception as e:
            print(e)

    def test_create(self):
        rows, action, result = self.trinodb.create(self.tablename, self.columns, partition='birthday')
        print(f"【rows】: {rows}, 【action】: {action}, 【result】: {result}")

    def test_insertsql(self):
        values = [['1', 'apple', 'beijing', '2012-01-23', '{"yuwen": 90, "shuxue": 20}'],
                  ['2', 'banana', 'shanghai', '2020-02-25 01:00:00', '{"yuwen": 91, "shuxue": 80}'],
                  ['3', 'chocolate', 'yunnan', '2020-06-14 23:00:05', '{"yuwen": 90, "shuxue": 90}'],
                  ['4', 'pizza', 'taiwan', '2020-05-15 23:08:25', '{"yuwen": 10, "shuxue": 21}'],
                  ['5', 'pizza', 'hebei', '2020-08-12 14:05:36', '{"yuwen": 30, "shuxue": 23}']]
        sqlcompile = SqlTrinoCompile(self.tablename)
        sql = sqlcompile.insert(self.columns, inserttype='value', values=values, chunksize=1000)
        print(sql)

    def test_insert(self):
        values = [['1', 'apple', 'beijing', '2012-01-23', '{"yuwen": 90, "shuxue": 20}'],
                  ['2', 'banana', 'shanghai', '2020-02-25 01:00:00', '{"yuwen": 91, "shuxue": 80}'],
                  ['3', 'chocolate', 'yunnan', '2020-06-14 23:00:05', '{"yuwen": 90, "shuxue": 90}'],
                  ['4', 'pizza', 'taiwan', '2020-05-15 23:08:25', '{"yuwen": 10, "shuxue": 21}'],
                  ['5', 'pizza', 'hebei', '2020-08-12 14:05:36', '{"yuwen": 30, "shuxue": 23}']]
        rows, action, result = self.trinodb.insert(self.tablename, self.columns, values=values, chunksize=1, verbose=1)
        print(f"【rows】: {rows}, 【action】: {action}, 【result】: {result}")

    def test_create_by_sql1(self):
        sql = f'''
        create table if not exists {self.tablename}_bysql
        (time varchar,
        adid varchar,
        dt varchar)
        with (partitioned_by = ARRAY['dt']);
        '''
        rows, action, result = self.trinodb.execute(sql, verbose=1)
        print(f"【rows】: {rows}, 【action】: {action}, 【result】: {result}")

    def test_create_by_sql2(self):
        sql = f'''
        create table if not exists {self.tablename}_bysql2 as 
        with test as
        (select * 
        from {self.tablename}
        limit 10),

        test1 as
        (select birthday as time, name as adid, substring(birthday, 1, 10) as dt
        from test
        )

        select * from test1
        ;
        '''
        rows, action, result = self.trinodb.execute(sql, verbose=1)
        print(f"【rows】: {rows}, 【action】: {action}, 【result】: {result}")

    def test_insert_by_sql(self):
        sql = f'''
            delete from {self.tablename}_bysql2;
            with test as
            (select * 
            from {self.tablename}
            limit 10),

            test1 as
            (select birthday as time, name as adid, substring(birthday, 1, 10) as dt
            from test
            )

            select * from test1
            ;
            insert into {self.tablename}_bysql2
            with test as
            (select * 
            from {self.tablename}
            limit 10),

            test1 as
            (select birthday as time, name as adid, substring(birthday, 1, 10) as dt
            from test
            )

            select * from test1
            ;
        '''
        rows, action, result = self.trinodb.execute(sql, verbose=1)
        print(f"【rows】: {rows}, 【action】: {action}, 【result】: {result}")

    def test_drop_by_sql(self):
        sql = f'''
        drop table {self.tablename}_bysql
        '''
        rows, action, result = self.trinodb.execute(sql)
        print(f"【rows】: {rows}, 【action】: {action}, 【result】: {result}")

    def test_select_by_sql(self):
        sql = f'''
            -- test1
            with test as
            (select * 
            from {self.tablename} 
            limit 10),

            test1 as
            (select birthday as time, name as adid, substring(birthday, 1, 10) as dt
            from test
            )

            select * from test1
            ;

            -- test2
            with test as
            (select * 
            from {self.tablename} 
            limit 10),

            test1 as
            (select birthday as time, name as adid, substring(birthday, 1, 10) as dt
            from test
            )

            select * from test1
            ;

            -- test3
            with test as
            (select * 
            from {self.tablename} 
            limit 10),

            test1 as
            (select birthday as time, name as adid, substring(birthday, 1, 10) as dt
            from test
            )

            select * from test1
            ;
        '''
        # print(sql)
        rows, action, result = self.trinodb.execute(sql, verbose=3)
        print(f"【rows】: {rows}, 【action】: {action}, 【result】: {result}")

    def test_alter_col(self):
        alter_cols = self.trinodb.alter_column(self.tablename, 'id', 'idx', 'int')
        print(alter_cols)

    def test_alter_tablecol(self):
        self.trinodb.alter_tablecol(self.tablename, colname='id', newname='idx', newtype='int', partition='birthday')

    def test_alter_tablecol1(self):
        self.trinodb.alter_tablecol(self.tablename, colname='idx', newname='idx', 
                                    newtype='int', sqlexpr='idx * 10', partition='birthday')
