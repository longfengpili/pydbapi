"""Use fresh interpreters so import side effects cannot be hidden by conftest."""
import os
from pathlib import Path
import subprocess
import sys
import textwrap


ROOT = Path(__file__).resolve().parents[2]


def run_python(tmp_path, source):
    prelude = '''
import os
from pathlib import Path
# Redirect legacy home-directory writes into the test sandbox for the red run.
original_expanduser = os.path.expanduser
os.path.expanduser = lambda path: str(Path.cwd()) if path == '~' else original_expanduser(path)
'''
    result = subprocess.run([sys.executable, '-c', textwrap.dedent(prelude) + textwrap.dedent(source)],
                            cwd=tmp_path, env={**os.environ, 'PYTHONPATH': str(ROOT)},
                            capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr


def test_import_preserves_application_logging_and_environment(tmp_path):
    run_python(tmp_path, '''
        import logging
        root = logging.getLogger()
        handler = logging.StreamHandler()
        root.addHandler(handler)
        root.setLevel(logging.ERROR)
        os.environ['NUMEXPR_MAX_THREADS'] = '3'
        import pydbapi
        assert root.handlers == [handler]
        assert root.level == logging.ERROR
        assert os.environ['NUMEXPR_MAX_THREADS'] == '3'
        assert not list(Path.cwd().iterdir())
    ''')


def test_sqlite_works_without_optional_drivers_or_notebook(tmp_path):
    run_python(tmp_path, '''
        import importlib.abc
        import sys
        blocked = {'pymysql', 'psycopg2', 'trino', 'IPython', 'colorlog'}
        class BlockOptional(importlib.abc.MetaPathFinder):
            def find_spec(self, fullname, path=None, target=None):
                if fullname.split('.')[0] in blocked:
                    raise ModuleNotFoundError(fullname, name=fullname.split('.')[0])
        sys.meta_path.insert(0, BlockOptional())
        from pydbapi.api import SqliteDB
        with SqliteDB(':memory:') as db:
            assert db.execute('select 7')[2].values == [(7,)]
        assert not (blocked & set(sys.modules))
        assert not list(Path.cwd().iterdir())
    ''')


def test_missing_driver_has_install_hint(tmp_path):
    run_python(tmp_path, '''
        import importlib.abc
        import sys
        class BlockMysql(importlib.abc.MetaPathFinder):
            def find_spec(self, fullname, path=None, target=None):
                if fullname == 'pymysql':
                    raise ModuleNotFoundError(fullname, name=fullname)
        sys.meta_path.insert(0, BlockMysql())
        try:
            from pydbapi.api import MysqlDB
        except ImportError as error:
            assert 'pip install pymysql' in str(error), str(error)
        else:
            raise AssertionError('Missing driver must be reported')
    ''')


def test_explicit_logging_is_scoped_and_repeatable(tmp_path):
    run_python(tmp_path, '''
        import logging
        import io
        root = logging.getLogger()
        root_handler = logging.StreamHandler(io.StringIO())
        root.addHandler(root_handler)
        from pydbapi.conf import configure_logging
        path = Path.cwd() / 'logs' / 'app.log'
        configure_logging(log_file=path)
        configure_logging(log_file=path)
        logging.getLogger('pydbapi.db.base').info('once-only-record')
        assert root.handlers == [root_handler]
        assert path.read_text(encoding='utf-8').count('once-only-record') == 1
        logging.shutdown()
    ''')


def test_notebook_sqlite_does_not_require_other_drivers(tmp_path):
    run_python(tmp_path, '''
        import importlib.abc
        import sys
        from IPython.core.interactiveshell import InteractiveShell
        class BlockDrivers(importlib.abc.MetaPathFinder):
            def find_spec(self, fullname, path=None, target=None):
                if fullname.split('.')[0] in {'pymysql', 'psycopg2', 'trino'}:
                    raise ModuleNotFoundError(fullname, name=fullname.split('.')[0])
        sys.meta_path.insert(0, BlockDrivers())
        shell = InteractiveShell(ipython_dir=str(Path.cwd() / 'ipython'))
        shell.run_line_magic('load_ext', 'pydbapi')
        shell.run_line_magic('dbconfig', "DBTYPE = 'sqlite'")
        shell.run_line_magic('dbconfig', "DATABASE = ':memory:'")
        frame = shell.run_cell_magic('pydbapi', '', 'select 9 as n')
        assert frame.n.tolist() == [9]
        shell.magics_manager.registry['PydbapiMagics'].dbapi.close()
    ''')


def test_setup_installs_all_dependencies_and_leaves_notebook_registration_explicit(tmp_path):
    run_python(tmp_path, '''
        import runpy
        from unittest.mock import patch
        from setuptools.dist import Distribution
        from setuptools.command.install import install
        sandbox = Path.cwd()
        os.chdir(os.environ['PYTHONPATH'])
        with patch('setuptools.setup') as setup:
            runpy.run_path('setup.py', run_name='__main__')
        config = setup.call_args.kwargs
        command = config.get('cmdclass', {}).get('install', install)(Distribution())
        with patch.object(install, 'run'), patch.object(Path, 'home', return_value=sandbox):
            command.run()
        assert not (sandbox / '.ipython').exists()
        core = {req.strip().lower() for req in config['install_requires']}
        assert core == {'sqlparse', 'pandas', 'tqdm', 'pymysql',
                        'psycopg2-binary', 'trino', 'ipython', 'colorlog'}
        assert not config.get('extras_require')
    ''')
