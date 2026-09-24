"""Database adapters, loaded only when their public names are requested."""
from importlib import import_module

_MODULES = {
    'RedshiftDB': 'redshift', 'SqlRedshiftCompile': 'redshift',
    'SqliteDB': 'sqlite', 'SqliteCompile': 'sqlite',
    'MysqlDB': 'mysql', 'SqlMysqlCompile': 'mysql',
    'TrinoDB': 'trino', 'SqlTrinoCompile': 'trino',
}
__all__ = list(_MODULES)


def __getattr__(name):
    if name not in _MODULES:
        raise AttributeError(f'module {__name__!r} has no attribute {name!r}')
    value = getattr(import_module(f'.{_MODULES[name]}', __name__), name)
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(__all__))
