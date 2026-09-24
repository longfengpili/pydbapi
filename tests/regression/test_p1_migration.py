from unittest.mock import Mock

import pytest

from pydbapi.api import SqliteDB
from pydbapi.model import ColumnModel, ColumnsModel


@pytest.fixture
def db(monkeypatch):
    monkeypatch.setattr('pydbapi.db.base.time.sleep', lambda _: None)
    with SqliteDB(':memory:', safe_rule=False) as database:
        yield database


def test_rename_failure_is_bounded_and_raised(db, monkeypatch):
    execute = Mock(side_effect=ValueError('rename denied'))
    monkeypatch.setattr(db, 'execute', execute)
    monkeypatch.setattr(db, 'get_columns', Mock(return_value=ColumnsModel()))
    with pytest.raises(RuntimeError, match='rename') as caught:
        db.alter_tablename('source', 'target', retries=2)
    assert execute.call_count == 2
    assert isinstance(caught.value.__cause__, ValueError)


def test_rename_does_not_accept_preexisting_target_after_failure(db):
    db.execute('create table source (id integer); create table target (id integer);')
    with pytest.raises(RuntimeError, match='rename'):
        db.alter_tablename('source', 'target', retries=1)
    assert db.get_columns('source').all_cols == ['id']


def test_rename_verification_retries_without_renaming_twice(db, monkeypatch):
    execute = Mock()
    monkeypatch.setattr(db, 'execute', execute)
    columns = ColumnsModel(ColumnModel('id', 'integer'))
    metadata = Mock(side_effect=[RuntimeError('metadata unavailable'), columns])
    monkeypatch.setattr(db, 'get_columns', metadata)
    db.alter_tablename('source', 'target', retries=2)
    assert execute.call_count == 1
    assert metadata.call_count == 2


def test_custom_column_expression_is_not_lost(db):
    db.execute('create table source (id integer)')
    columns = db.alter_column('source', 'id', sqlexpr='id + 1')
    assert columns.get_column_by_name('id').sqlexpr == 'id + 1'


def test_unknown_migration_column_raises(db):
    db.execute('create table source (id integer)')
    with pytest.raises(ValueError, match='missing'):
        db.alter_column('source', 'missing', newname='other')


def test_successful_migration_returns_backup_and_preserves_original_rows(db):
    db.execute('create table source (id integer); insert into source values (1), (2); '
               'create table staging (id integer);')
    cols = ColumnsModel(ColumnModel('id', 'integer', sqlexpr='id + 10'))
    backup = db.alter_tablecol_base('source', 'staging', cols)
    assert db.execute('select * from source order by id')[2].values == [(11,), (12,)]
    assert db.execute(f'select * from {backup} order by id')[2].values == [(1,), (2,)]


def test_migration_rejects_populated_staging_before_renaming(db):
    db.execute('create table source (id integer); create table staging (id integer); '
               'insert into staging values (99)')
    with pytest.raises(ValueError, match='empty'):
        db.alter_tablecol_base('source', 'staging', ColumnsModel(ColumnModel('id', 'integer')))
    assert db.get_columns('source').all_cols == ['id']


def test_migration_failure_keeps_backup_and_reports_stage(db):
    db.execute('create table source (id integer); insert into source values (1); '
               'create table staging (id integer);')
    cols = ColumnsModel(ColumnModel('id', 'integer', sqlexpr='missing_column'))
    with pytest.raises(RuntimeError, match='copy') as caught:
        db.alter_tablecol_base('source', 'staging', cols)
    backups = db.execute("select name from sqlite_master where name like 'source_backup_%'")[2].values
    assert len(backups) == 1
    backup = backups[0][0]
    assert backup in str(caught.value)
    assert db.execute(f'select * from {backup}')[2].values == [(1,)]
    assert db.execute('select * from staging')[2].values == []
