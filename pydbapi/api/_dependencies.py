from importlib import import_module


def require_driver(module, package):
    """Report missing drivers without masking failures in their dependencies."""
    try:
        return import_module(module)
    except ModuleNotFoundError as error:
        if error.name != module.split('.')[0]:
            raise
        raise ImportError(f'{module} requires: pip install {package}') from error
