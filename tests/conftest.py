"""External database tests are opt-in; pytest owns test logging."""


def pytest_addoption(parser):
    parser.addoption('--run-integration', action='store_true', default=False,
                     help='Run legacy MySQL/Trino tests against explicitly configured test databases')


def pytest_ignore_collect(collection_path, config):
    if not config.getoption('--run-integration'):
        return collection_path.name in ('mysql', 'trino') and collection_path.parent.name == 'tests'
    return None
