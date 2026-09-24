from datetime import date, timedelta
import sqlite3
from unittest.mock import Mock

import pytest

from pydbapi.api import MysqlDB, RedshiftDB, SqliteDB, TrinoDB
from pydbapi.model import ColumnModel, ColumnsModel
from pydbapi.sql import SqlStatement, SqlFileParse


@pytest.fixture
def db():
    with SqliteDB(':memory:', safe_rule=False) as database:
        yield database


@pytest.mark.parametrize('cls,target,placeholder', [
    (MysqlDB, 'pydbapi.api.mysql.pymysql.connect', '%s'),
    (RedshiftDB, 'pydbapi.api.redshift.psycopg2.connect', '%s'),
    (TrinoDB, 'pydbapi.api.trino.connect', '?'),
])
def test_insert_values_reach_driver_unchanged(monkeypatch, cls, target, placeholder):
    cursor = Mock(description=None, rowcount=1)
    cursor.fetchall.return_value = []
    conn = Mock(transaction=None)
    conn.cursor.return_value = cursor
    monkeypatch.setattr(target, Mock(return_value=conn))
    sql = f'insert into items (value) values ({placeholder});'
    payload = "not SQL; ' \\1"
    db = cls('host', 'user', 'password', 'database', safe_rule=False)
    db.insert('items', ColumnsModel(ColumnModel('value', 'text')), values=[[payload]])
    if cls is TrinoDB:
        sql = sql.rstrip(';')
    cursor.execute.assert_called_once_with(sql, (payload,))


def test_insert_binds_text_regardless_of_column_type(db):
    db.execute('create table items (value text, amount integer)')
    columns = ColumnsModel(ColumnModel('value', 'text'), ColumnModel('amount', 'integer'))
    rows = [["a'; -- 中文", 1], ['semicolon; newline\n  text', None], ['last', 3]]
    result = db.insert('items', columns, values=rows, chunksize=2)[2]
    assert result.error is None
    assert db.execute('select * from items')[2].values == [tuple(row) for row in rows]


def test_late_insert_chunk_failure_rolls_back_earlier_chunks(db):
    db.execute('create table items (id integer unique)')
    columns = ColumnsModel(ColumnModel('id', 'integer'))
    result = db.insert('items', columns, values=[[1], [2], [1]], chunksize=2, ehandling='pass')[2]
    assert result.error is not None
    assert db.execute('select * from items')[2].values == []


def test_insert_checks_all_row_widths_before_connecting():
    db = SqliteDB(':memory:', safe_rule=False)
    with pytest.raises(ValueError, match='columns'):
        db.insert('items', ColumnsModel(ColumnModel('id')), values=[[1], [2, 3]], chunksize=1)
    assert db._conn is None


@pytest.mark.skipif(not hasattr(sqlite3.Connection, 'setlimit'), reason='sqlite3.setlimit requires Python 3.11+')
def test_insert_chunks_respect_sqlite_parameter_limit(db):
    db.execute('create table items (id integer, value text)')
    db.get_conn().setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 5)
    cols = ColumnsModel(ColumnModel('id', 'integer'), ColumnModel('value', 'text'))
    rows = [[i, 'value'] for i in range(8)]
    db.insert('items', cols, values=rows)
    assert db.execute('select * from items order by id')[2].values == [(i, 'value') for i in range(8)]


@pytest.mark.parametrize('sql', ['START TRANSACTION', '/* first */ START /* gap */ TRANSACTION;'])
def test_explicit_transaction_start_rejected_before_connecting(monkeypatch, sql):
    db = MysqlDB('host', 'user', 'password', 'database')
    monkeypatch.setattr(db, 'get_conn', Mock(side_effect=AssertionError('connected too soon')))
    with pytest.raises(ValueError, match='transaction'):
        db.execute(sql)


def test_expression_rejects_string_format_allocation():
    with pytest.raises(ValueError, match='Unsupported'):
        SqlFileParse('unused.sql').parse_argument("value = '%1000000s' % 1", {})


def test_template_names_are_exact_and_values_not_reinterpreted():
    stmt = SqlStatement('select $date_max, $date, $date; -- leave $comment')
    stmt.substitute_params(date='\\1 $date_max', date_max='42')
    assert stmt.sql == 'select 42, \\1 $date_max, \\1 $date_max; -- leave $comment'


@pytest.mark.parametrize('value', [0, False, '', None])
def test_file_arguments_accept_falsy_overrides(value):
    parser = SqlFileParse('unused.sql')
    assert parser.update_arguments({'value': 'old'}, value=value)['value'] == value


def test_file_expressions_support_dates_and_semicolons_in_strings(tmp_path):
    file = tmp_path / 'dates.sql'
    file.write_text('#【arguments】#\ntext = "a;b"\nstart = date(2024, 1, 1)\n'
                    'end = start + timedelta(days=2)\n#【arguments】#\n'
                    '###\n--【report verbose2 epass】\nselect $end;\n###', encoding='utf-8')
    args, blocks = SqlFileParse(file).get_filesqls()
    assert args['text'] == 'a;b'
    assert args['end'] == "'2024-01-03'"
    assert 'report verbose2 epass' in next(iter(blocks))


@pytest.mark.parametrize('expression', ["__import__('os')", '(1).__class__',
                                        "open('should_not_exist', 'w')", '[x for x in (1, 2)]'])
def test_file_expressions_reject_code_execution(expression):
    with pytest.raises(ValueError, match='Unsupported'):
        SqlFileParse('unused.sql').parse_argument('value = ' + expression, {})


def test_file_expression_can_reference_previous_date():
    assert SqlFileParse('unused.sql').parse_argument('value = start - timedelta(days=1)',
                                                    {'start': date(2024, 1, 2)}) == ('value', date(2024, 1, 1))


def test_file_missing_name_has_clear_error():
    with pytest.raises(NameError, match='missing'):
        SqlFileParse('unused.sql').parse_argument('value = missing + 1', {})


def test_sql_file_delimiters_inside_literals_do_not_split_blocks(tmp_path):
    file = tmp_path / 'literal.sql'
    file.write_text("###\nselect '###' as value;\n###", encoding='utf-8')
    _, blocks = SqlFileParse(file).get_filesqls()
    assert next(iter(blocks.values()))[0].sql == "select '###' as value;"


def test_file_epass_description_controls_block_error_handling(db, tmp_path):
    file = tmp_path / 'flags.sql'
    file.write_text('###\n--【failing epass】\nselect * from missing;\n###\n'
                    '###\n--【next】\nselect 7 as value;\n###', encoding='utf-8')
    blocks = list(db.file_exec(file).values())
    assert blocks[0].error is not None
    assert blocks[1].values == [(7,)]


def test_file_verbose_description_uses_documented_flag(db, tmp_path, monkeypatch):
    file = tmp_path / 'flags.sql'
    file.write_text('###\n--【report verbose】\nselect 7;\n###', encoding='utf-8')
    progress = Mock()
    monkeypatch.setattr(db, 'handle_progress_logging', progress)
    db.file_exec(file)
    assert progress.call_args.args[1] == 1


@pytest.mark.parametrize('chunksize', [0, -1, True, 1.5])
def test_bad_chunk_size_rejected_before_connection(chunksize):
    db = SqliteDB(':memory:', safe_rule=False)
    with pytest.raises(ValueError, match='chunksize'):
        db.insert('items', ColumnsModel(ColumnModel('id')), values=[[1]], chunksize=chunksize)
    assert db._conn is None
