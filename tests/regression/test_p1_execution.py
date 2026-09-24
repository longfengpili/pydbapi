"""Execution contracts: real SQLite state, with external driver boundaries stubbed."""
import sqlite3
from itertools import islice
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from pydbapi.api import MysqlDB, RedshiftDB, SqliteDB, TrinoDB
from pydbapi.model import ColumnModel, ColumnsModel, ResModel


@pytest.fixture
def db(tmp_path):
    database = SqliteDB(str(tmp_path / 'execution.db'), safe_rule=False)
    yield database
    if database._conn is not None:
        database._conn.close()


def test_dml_metadata_survives_cursor_close(db):
    db.execute('create table items (id integer)')
    cursor, action, result = db.execute('insert into items values (1), (2)')
    assert (action, result.action, result.rowcount) == ('insert', 'insert', 2)
    assert result.error is None
    assert result.values == []
    assert result.to_dataframe().shape == (0, 0)
    with pytest.raises(sqlite3.ProgrammingError, match='closed'):
        cursor.execute('select 1')


def test_empty_results_export_without_columns(tmp_path):
    result = ResModel(None, [])
    assert result.to_dataframe().shape == (0, 0)
    assert result.to_insert_values() == ''
    output = tmp_path / 'empty.csv'
    result.to_csv(output)
    assert output.read_text(encoding='utf-8') == ''


@pytest.mark.parametrize('count,expected', [(None, [(1,), (2,)]), (0, []), (1, [(1,)])])
def test_count_controls_returned_rows_but_keeps_columns(db, count, expected):
    result = db.execute('select 1 as id union all select 2', count=count)[2]
    assert result.values == expected
    assert result.cols.all_cols == ['id']
    assert result.to_dataframe().columns.tolist() == ['id']


@pytest.mark.parametrize('sql,options', [
    ('', {}), ('  -- comment only', {}), (';;;', {}), ('; -- comment', {}),
    ('select 1', {'count': -1}), ('select 1', {'count': True}),
    ('select 1', {'count': 1.5}), ('select 1', {'ehandling': 'raises'}),
    ('select 1', {'ehandling': None}), ('select 1', {'verbose': -1}),
])
def test_invalid_input_is_rejected_before_connecting(db, sql, options):
    with pytest.raises((ValueError, TypeError)):
        db.execute(sql, **options)
    assert db._conn is None


def test_query_followed_by_dml_returns_final_empty_result(db):
    db.execute('create table items (id integer)')
    _, action, result = db.execute('select 99; insert into items values (1); -- trailing')
    assert action == result.action == 'insert'
    assert result.values == []
    assert result.rowcount == 1


def test_trailing_empty_statements_do_not_erase_result(db):
    assert db.execute('select 5; ; -- comment')[2].values == [(5,)]


def test_pragma_returns_rows_without_select_keyword(db):
    db.execute('create table items (id integer)')
    _, action, result = db.execute('pragma table_info(items)')
    assert action == result.action == 'pragma'
    assert result.values[0][1] == 'id'


def test_sqlite_foreign_keys_pragma_really_enables_constraints(db):
    db.execute('pragma foreign_keys=ON')
    assert db.execute('pragma foreign_keys')[2].values == [(1,)]
    db.execute('create table parent (id integer primary key); '
               'create table child (parent_id integer references parent(id));')
    with pytest.raises(ValueError) as caught:
        db.execute('insert into child values (42)')
    assert isinstance(caught.value.__cause__, sqlite3.IntegrityError)


def test_sqlite_vacuum_runs_standalone(db):
    db.execute('create table items (id integer)')
    assert db.execute('vacuum')[2].error is None


@pytest.mark.parametrize('sql', ['pragma foreign_keys=ON; select 1',
                                 'create table items (id integer); vacuum',
                                 'begin; select 1; commit'])
def test_incompatible_transaction_batch_rejected_before_connecting(db, sql):
    with pytest.raises(ValueError, match='transaction'):
        db.execute(sql)
    assert db._conn is None


def test_failed_batch_rolls_back_ddl_and_dml_and_keeps_cause(db):
    with pytest.raises(ValueError) as caught:
        db.execute('create table items (id integer); insert into items values (1); select * from missing')
    assert isinstance(caught.value.__cause__, sqlite3.OperationalError)
    assert db.execute("select name from sqlite_master where name='items'")[2].values == []


def test_pass_aborts_batch_rolls_back_and_reports_error(db):
    db.execute('create table items (id integer unique)')
    _, action, result = db.execute(
        'insert into items values (1); insert into items values (1); insert into items values (2)',
        ehandling='pass')
    assert action == 'insert'
    assert isinstance(result.error, ValueError)
    assert isinstance(result.error.__cause__, sqlite3.IntegrityError)
    assert result.values == []
    assert db.execute('select * from items')[2].values == []
    assert db.execute('insert into items values (3)')[2].rowcount == 1


def test_file_blocks_commit_independently(db, tmp_path):
    db.execute('create table items (id integer)')
    path = tmp_path / 'blocks.sql'
    path.write_text('###\ninsert into items values (1);\n###\n'
                    '###\ninsert into items values (2); select * from missing;\n###', encoding='utf-8')
    results = db.file_exec(path, ehandling='pass')
    assert [result.error is None for result in results.values()] == [True, False]
    assert db.execute('select * from items')[2].values == [(1,)]


def test_file_rejects_invalid_error_mode_before_any_work(db, tmp_path):
    path = tmp_path / 'blocks.sql'
    path.write_text('###\ncreate table items (id integer);\n###', encoding='utf-8')
    with pytest.raises(ValueError, match='ehandling'):
        db.file_exec(path, ehandling='raises')
    assert db._conn is None


def test_insert_helper_returns_suppressed_error_without_reporting_success(db):
    db.execute('create table items (id integer unique)')
    cols = ColumnsModel(ColumnModel('id', 'integer'))
    result = db.insert('items', cols, values=[[1], [1]], chunksize=1, ehandling='pass')[2]
    assert result.error is not None
    assert db.execute('select * from items')[2].values == []


def test_close_is_lazy_idempotent_and_allows_reconnect(db):
    db.close()
    assert db._conn is None
    db.execute('create table items (id integer)')
    original = db.get_conn()
    db.close()
    db.close()
    with pytest.raises(sqlite3.ProgrammingError, match='closed'):
        original.execute('select 1')
    assert db.get_conn() is not original
    assert db.execute('select * from items')[2].values == []


def test_context_closes_connection_but_each_execute_commits(db):
    with pytest.raises(RuntimeError, match='application'):
        with db as same:
            assert same is db
            db.execute('create table items (id integer); insert into items values (4)')
            connection = db.get_conn()
            raise RuntimeError('application')
    with pytest.raises(sqlite3.ProgrammingError, match='closed'):
        connection.execute('select 1')
    assert db.execute('select * from items')[2].values == [(4,)]


@pytest.fixture(params=[MysqlDB, RedshiftDB, TrinoDB])
def external_db(request, monkeypatch):
    cls = request.param
    target = {MysqlDB: 'pydbapi.api.mysql.pymysql.connect',
              RedshiftDB: 'pydbapi.api.redshift.psycopg2.connect',
              TrinoDB: 'pydbapi.api.trino.connect'}[cls]
    cursor = Mock(description=None, rowcount=2)
    cursor.fetchall.return_value = []
    cursor.fetchmany.return_value = []
    connection = Mock(transaction=None)
    connection.cursor.return_value = cursor
    monkeypatch.setattr(target, Mock(return_value=connection))
    database = cls('host', 'user', 'password', 'database', safe_rule=False)
    return database, connection, cursor


def test_external_dml_does_not_fetch_nonexistent_result_set(external_db):
    db, conn, cursor = external_db
    if db.dbtype != 'trino':
        cursor.fetchall.side_effect = RuntimeError('no results to fetch')
    result = db.execute('update items set id=2')[2]
    assert result.rowcount == 2
    assert result.values == []
    cursor.close.assert_called_once()
    conn.commit.assert_called_once()


def test_external_result_metadata_captured_before_close(external_db):
    db, _, cursor = external_db
    cursor.description = ([SimpleNamespace(name='id', type_code='integer')] if db.dbtype == 'trino'
                          else [('id', 23, None, None, None, None, None)])
    cursor.fetchall.return_value = [(8,)]
    cursor.close.side_effect = lambda: setattr(cursor, 'rowcount', -1)
    result = db.execute('select 8 as id')[2]
    assert result.values == [(8,)]
    assert result.rowcount == 2
    cursor.close.assert_called_once()


def test_external_failure_rolls_back_supported_transaction(external_db):
    db, conn, cursor = external_db
    conn.transaction = object()  # Trino explicit transaction mode.
    original = RuntimeError('driver failure')
    cursor.execute.side_effect = original
    with pytest.raises(ValueError) as caught:
        db.execute('insert into items values (1)')
    assert caught.value.__cause__ is original
    conn.rollback.assert_called_once()
    conn.commit.assert_not_called()
    cursor.close.assert_called_once()


def test_commit_failure_rolls_back_and_closes(external_db):
    db, conn, cursor = external_db
    conn.transaction = object()
    conn.commit.side_effect = RuntimeError('commit failed')
    with pytest.raises(RuntimeError, match='commit failed'):
        db.execute('update items set id=2')
    conn.rollback.assert_called_once()
    cursor.close.assert_called_once()


def test_pass_does_not_suppress_rollback_failure(external_db):
    db, conn, cursor = external_db
    conn.transaction = object()
    cursor.execute.side_effect = RuntimeError('execute failed')
    conn.rollback.side_effect = RuntimeError('rollback failed')
    with pytest.raises(RuntimeError, match='rollback failed'):
        db.execute('update items set id=2', ehandling='pass')
    cursor.close.assert_called_once()
    conn.close.assert_called_once()
    assert db._conn is None


def test_cursor_creation_failure_rolls_back_without_suppressing(external_db):
    db, conn, cursor = external_db
    conn.transaction = object()
    conn.cursor.side_effect = RuntimeError('cannot create cursor')
    with pytest.raises(RuntimeError, match='cannot create cursor'):
        db.execute('select 1', ehandling='pass')
    conn.rollback.assert_called_once()
    conn.commit.assert_not_called()


def test_cursor_close_failure_before_commit_rolls_back(external_db):
    db, conn, cursor = external_db
    conn.transaction = object()
    cursor.close.side_effect = RuntimeError('cannot close cursor')
    with pytest.raises(RuntimeError, match='cannot close cursor'):
        db.execute('update items set id=2')
    conn.rollback.assert_called_once()
    conn.commit.assert_not_called()


def test_secondary_close_failure_does_not_mask_sql_error(external_db):
    db, conn, cursor = external_db
    conn.transaction = object()
    original = RuntimeError('SQL failed')
    cursor.execute.side_effect = original
    cursor.close.side_effect = RuntimeError('close also failed')
    with pytest.raises(ValueError) as caught:
        db.execute('select 1')
    assert caught.value.__cause__ is original
    conn.rollback.assert_called_once()


def test_interrupt_is_rolled_back_and_never_suppressed(external_db):
    db, conn, cursor = external_db
    conn.transaction = object()
    cursor.execute.side_effect = KeyboardInterrupt()
    with pytest.raises(KeyboardInterrupt):
        db.execute('select 1', ehandling='pass')
    conn.rollback.assert_called_once()
    cursor.close.assert_called_once()


def test_limited_query_drains_stream_before_commit(external_db):
    db, conn, cursor = external_db
    cursor.description = ([SimpleNamespace(name='id', type_code='integer')] if db.dbtype == 'trino'
                          else [('id', 23, None, None, None, None, None)])
    rows = iter([(1,), (2,), (3,)])
    cursor.fetchmany.side_effect = lambda count: list(islice(rows, count))

    def commit():
        assert list(rows) == [], 'commit must not leave a streaming result unfinished'

    conn.commit.side_effect = commit
    result = db.execute('select id from items', count=1)[2]
    assert result.values == [(1,)]
    cursor.close.assert_called_once()


def test_late_fetch_failure_rolls_back(external_db):
    db, conn, cursor = external_db
    conn.transaction = object()
    cursor.description = ([SimpleNamespace(name='id', type_code='integer')] if db.dbtype == 'trino'
                          else [('id', 23, None, None, None, None, None)])
    cursor.fetchmany.side_effect = [[(1,)], RuntimeError('stream failed')]
    result = db.execute('select id from items', count=1, ehandling='pass')[2]
    assert str(result.error) == 'stream failed'
    assert result.values == []
    conn.rollback.assert_called_once()
    conn.commit.assert_not_called()


def test_trino_autocommit_failure_does_not_attempt_rollback(monkeypatch):
    cursor = Mock()
    cursor.execute.side_effect = RuntimeError('failed')
    connection = Mock(transaction=None)
    connection.cursor.return_value = cursor
    monkeypatch.setattr('pydbapi.api.trino.connect', Mock(return_value=connection))
    db = TrinoDB('host', 'user', 'password', 'database')
    result = db.execute('select 1', ehandling='pass')[2]
    assert result.error is not None
    connection.rollback.assert_not_called()
    cursor.close.assert_called_once()
