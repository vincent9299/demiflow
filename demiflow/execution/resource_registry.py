"""Compatibility registration for platform modules with action-scoped pools.

New Dataset nodes should use explicit resources. Legacy modules register their
own cleanup instead of teaching Dataset about their private global variables.
"""
_cleanups = {}


def register_stream_cleanup(name, callback):
    _cleanups[name] = callback


def stream_cleanups():
    return tuple(_cleanups.values())
