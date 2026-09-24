"""Keep package logging inside the test process, without user-directory files."""
import logging.config
from unittest.mock import patch


# Importing pydbapi configures file handlers; tests only need console logging.
with patch.object(logging.config, 'dictConfig'):
    from pydbapi.conf import LOGGING_CONFIG

LOGGING_CONFIG['handlers'] = {'console': LOGGING_CONFIG['handlers']['console']}
for logger in LOGGING_CONFIG['loggers'].values():
    logger['handlers'] = ['console']
logging.config.dictConfig(LOGGING_CONFIG)


def pytest_addoption(parser):
    parser.addoption('--run-integration', action='store_true', default=False,
                     help='Run legacy MySQL/Trino tests against explicitly configured test databases')


def pytest_ignore_collect(collection_path, config):
    if not config.getoption('--run-integration'):
        return collection_path.name in ('mysql', 'trino') and collection_path.parent.name == 'tests'
    return None
