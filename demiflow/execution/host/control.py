"""Lazy named control contracts; every export is its original native object."""
from importlib import import_module
from ._surface import SURFACE

_EXPORTS = SURFACE['exports']['control']
__all__ = tuple(_EXPORTS)


def __getattr__(name):
    if name not in _EXPORTS:
        raise AttributeError(name)
    module, member = _EXPORTS[name]
    value = getattr(import_module(module), member)
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(__all__))
