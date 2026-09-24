"""Regression coverage for connection isolation and executable SQL entry points."""
from unittest.mock import Mock
import sqlite3

import pytest
from IPython.core.interactiveshell import InteractiveShell

from pydbapi.api import MysqlDB, RedshiftDB, SqliteDB, TrinoDB
from pydbapi.sql import SqlStatement, SqlStatements


@pytest.fixture
def db():
    database = SqliteDB(':memory:', safe_rule=False)
    yield database
    database.get_conn().close()


def test_sqlite_connections_are_isolated(tmp_path):
    first = SqliteDB(str(tmp_path / 'first.db'))
    second = SqliteDB(str(tmp_path / 'second.db'))
    try:
        first.execute('create table marker (value integer); insert into marker values (42);')
        second.execute('create table marker (value integer);')
        assert first.get_conn() is first.get_conn()
        assert first.get_conn() is not second.get_conn()
        assert first.execute('select * from marker')[2].values == [(42,)]
        assert second.execute('select * from marker')[2].values == []
    finally:
        first.get_conn().close()
        second.get_conn().close()


@pytest.mark.parametrize('cls,target,field', [
    (MysqlDB, 'pydbapi.api.mysql.pymysql.connect', 'database'),
    (RedshiftDB, 'pydbapi.api.redshift.psycopg2.connect', 'database'),
    (TrinoDB, 'pydbapi.api.trino.connect', 'schema'),
])
def test_driver_connections_are_instance_owned(monkeypatch, cls, target, field):
    connect = Mock(side_effect=[Mock(), Mock()])
    monkeypatch.setattr(target, connect)
    first = cls('host', 'user', 'password', 'first')
    second = cls('host', 'user', 'password', 'second')
    assert first.get_conn() is first.get_conn()
    assert first.get_conn() is not second.get_conn()
    assert [call.kwargs[field] for call in connect.call_args_list] == ['first', 'second']


@pytest.mark.parametrize('cls,args,kwargs', [
    (SqliteDB, (':memory:',), {'database': ':memory:'}),
    (MysqlDB, ('host', 'user', 'password', 'first'),
     dict(host='host', user='user', password='password', database='first')),
    (RedshiftDB, ('host', 'user', 'password', 'first'),
     dict(host='host', user='user', password='password', database='first')),
    (TrinoDB, ('host', 'user', 'password', 'first'),
     dict(host='host', user='user', password='password', database='first')),
])
def test_singleton_rejects_different_configuration(monkeypatch, cls, args, kwargs):
    monkeypatch.setattr(cls, '_instance', None, raising=False)
    instance = cls.get_instance(*args)
    assert cls.get_instance(**kwargs) is instance
    assert cls.get_instance() is instance
    with pytest.raises(ValueError, match='configuration'):
        cls.get_instance(**dict(kwargs, database='different'))


def test_redshift_columns():
    database = RedshiftDB('host', 'user', 'password', 'database')
    cursor = Mock(description=[('id', 23, None, None, None, None, None)])
    assert database.cur_columns(cursor).all_cols == ['id']
    cursor.description = None
    assert database.cur_columns(cursor) is None


def test_trino_execution_omits_only_statement_delimiter(monkeypatch):
    connection = Mock()
    cursor = connection.cursor.return_value
    cursor.description = None
    cursor.fetchall.return_value = []
    monkeypatch.setattr('pydbapi.api.trino.connect', Mock(return_value=connection))
    database = TrinoDB('host', 'user', 'password', 'database')
    database.execute("SELECT 'a;  b' AS value; -- trailing ; comment")
    cursor.execute.assert_called_once_with("SELECT 'a;  b' AS value -- trailing ; comment")


def test_trino_singleton_accepts_same_session_object(monkeypatch):
    monkeypatch.setattr(TrinoDB, '_instance', None, raising=False)
    session = Mock()
    database = TrinoDB.get_instance('host', 'user', 'password', 'database', http_session=session)
    assert TrinoDB.get_instance('host', 'user', 'password', 'database', http_session=session) is database


@pytest.mark.parametrize('value', ['a;b', 'a  b', 'a\n  b\n\nc', "it''s;  fine", '-- ; /* text */'])
def test_sql_literal_roundtrip(db, value):
    sql = f"-- comment with ;\nSELECT '{value}' AS value;"
    assert len(SqlStatements(sql)) == 1
    result = db.execute(sql)[2]
    assert result.values == [(value.replace("''", "'"),)]


def test_slice_and_multistatement_preserve_literals(db):
    statements = SqlStatements("select 'first;  value';\nselect 'second\n  value';")
    assert len(statements) == 2
    assert db.execute(statements[:1])[2].values == [('first;  value',)]
    assert db.execute(statements)[2].values == [('second\n  value',)]


def test_execution_preserves_comments_and_case(db):
    sql = "/*+ example_hint */ SELECT 'Keep  Case' AS value;"
    traced = []
    db.get_conn().set_trace_callback(traced.append)
    db.execute(sql)
    assert sql in traced


def test_trailing_comment_does_not_replace_query_result(db):
    assert db.execute('select 42;\n-- trailing ; comment')[2].values == [(42,)]


@pytest.mark.parametrize('sql,index,expected', [
    ('with first as (select 1 as n) select * from first;', 1, [(1,)]),
    ('with first as (select 1 as n), second as (select n+1 as n from first) select * from second;', 1, [(1,)]),
    ('with first as (select 1 as n), second as (select n+1 as n from first) select * from second;', 2, [(2,)]),
    ('with "first cte"(n) as (select 3) select * from "first cte";', 1, [(3,)]),
    ('with recursive counter(n) as (select 1 union all select n+1 from counter where n<3) select * from counter;', 1, [(1,), (2,), (3,)]),
    ("-- CTE comment\nwith a as (select 'x;  (y)' as n),\n-- next\nb as (select n from (select * from a)) select * from b;", 2, [('x;  (y)',)]),
])
def test_cte_debug_sql_executes(db, sql, index, expected):
    debug = SqlStatement(sql).get_with_testsql(index)
    assert db.execute(debug.sql)[2].values == expected


@pytest.mark.parametrize('index', [0, -1, 2])
def test_cte_index_bounds(index):
    with pytest.raises(ValueError, match='index'):
        SqlStatement('with first as (select 1) select * from first').get_with_testsql(index)


def test_cte_requires_with():
    with pytest.raises(ValueError, match='CTE'):
        SqlStatement('select 1').get_with_testsql()


def test_file_debug_uses_first_block_and_one_based_index(db, tmp_path):
    path = tmp_path / 'debug.sql'
    path.write_text('###\nwith first as (select 7 as n) select * from first;\n###\n'
                    '###\nselect * from missing_table;\n###', encoding='utf-8')
    results = db.file_exec(path, with_test=True)
    assert len(results) == 1
    assert next(iter(results.values())).values == [(7,)]


def test_file_debug_selects_second_cte(db, tmp_path):
    path = tmp_path / 'debug.sql'
    path.write_text('###\nwith a as (select 7 as n), b as (select n+1 as n from a) select * from b;\n###',
                    encoding='utf-8')
    results = db.file_exec(path, with_test=True, with_snum=2)
    assert next(iter(results.values())).values == [(8,)]


def test_file_debug_rejects_multiple_statements(db, tmp_path):
    path = tmp_path / 'debug.sql'
    path.write_text('###\nwith a as (select 7) select * from a; select 2;\n###', encoding='utf-8')
    with pytest.raises(ValueError, match='exactly one statement'):
        db.file_exec(path, with_test=True)


def test_ipython_extension_executes_sqlite_without_credentials(monkeypatch, tmp_path):
    monkeypatch.setattr('builtins.input', Mock(side_effect=AssertionError('Unexpected prompt')))
    shell = InteractiveShell(ipython_dir=str(tmp_path / 'ipython'))
    shell.run_line_magic('load_ext', 'pydbapi')
    shell.run_line_magic('dbconfig', "DBTYPE = 'sqlite'")
    shell.run_line_magic('dbconfig', "DATABASE = ':memory:'")
    magic = shell.magics_manager.registry['PydbapiMagics']
    try:
        shell.run_cell_magic('pydbapi', '', 'create table sample (n integer); select 1 as n;')
        frame = shell.run_cell_magic('pydbapi', '-d answer', 'insert into sample values (9); select * from sample;')
        assert frame['n'].tolist() == [9]
        assert shell.user_ns['answer'] is frame
        old_connection = magic.dbapi.get_conn()
        shell.run_line_magic('dbconfig', f'DATABASE = {str(tmp_path / "new.db")!r}')
        frame = shell.run_cell_magic('pydbapi', '', "select count(*) as n from sqlite_master where name='sample';")
        assert frame['n'].tolist() == [0]
        with pytest.raises(sqlite3.ProgrammingError, match='closed'):
            old_connection.execute('select 1')
    finally:
        magic.dbapi.get_conn().close()
