"""Explicit package-scoped logging. Importing this module performs no I/O."""
import logging
from pathlib import Path
from .myhandlers import MakeFileHandler

AUTO_RULES = ['test_xu', 'tmp']
REDSHIFT_AUTO_RULES = AUTO_RULES + ['_data_aniland']
_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# Compatibility for explicit logging.config.dictConfig callers. Console only.
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {'simple': {'format': _FORMAT}},
    'handlers': {'console': {'class': 'logging.StreamHandler', 'formatter': 'simple'}},
    'loggers': {'pydbapi': {'handlers': ['console'], 'level': 'INFO', 'propagate': False}},
}


def configure_logging(*, level=logging.INFO, log_file=None, color=False):
    """Configure pydbapi only, replacing handlers created by earlier calls.

    Default: console. log_file opts into daily UTF-8 rotation (30 backups).
    Application/root handlers are untouched.
    """
    formatter = logging.Formatter(_FORMAT)
    console_formatter = formatter
    if color:
        try:
            from colorlog import ColoredFormatter
        except ModuleNotFoundError as error:
            if error.name != 'colorlog':
                raise
            raise ImportError('Colored logging requires: pip install colorlog') from error
        console_formatter = ColoredFormatter('%(log_color)s' + _FORMAT)
    console = logging.StreamHandler()
    console.setFormatter(console_formatter)
    handlers = [console]
    try:
        if log_file is not None:
            file_handler = MakeFileHandler(str(Path(log_file).resolve()), when='d',
                                           backupCount=30, encoding='utf-8')
            file_handler.setFormatter(formatter)
            handlers.append(file_handler)
        for handler in handlers:
            handler.setLevel(level)
    except Exception:
        for handler in handlers:
            handler.close()
        raise
    logger = logging.getLogger('pydbapi')
    for handler in list(logger.handlers):
        if getattr(handler, '_pydbapi_configured', False):
            logger.removeHandler(handler)
            handler.close()
    for handler in handlers:
        handler._pydbapi_configured = True
        logger.addHandler(handler)
    logger.setLevel(level)
    logger.propagate = False
    return logger
