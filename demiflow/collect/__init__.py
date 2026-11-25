"""Data acquisition behind the Dataset operators: web search, raw fetch, documents, images and reading.

The engine reaches these modules through ``Dataset.search_web``, ``fetch``, ``decode_document``,
``register_documents``, ``read_documents``, ``encode_image`` and ``parse_wikitext``. ``cosio`` signs and reads
COS objects for the fetch transport. Mechanism lives here; sources, limits and prompts belong to the caller.
"""

import importlib as _il
import sys as _sys

_LAZY = ("cosio",)


def __getattr__(name):
    """Submodules and contract names are imported on first use."""
    if name in _LAZY and f"{__name__}.{name}" in _sys.modules:
        return _sys.modules[f"{__name__}.{name}"]
    if name in _LAZY:
        return _il.import_module(f"{__name__}.{name}")
    if name in {'FetchPolicy', 'HttpSource', 'CosSource'}:
        return getattr(_il.import_module(f'{__name__}.raw_contract'), name)
    if name in {'SearchConfig', 'Secret', 'search_source_inventory'}:
        return getattr(_il.import_module(f'{__name__}.native_search'), name)
    raise AttributeError(name)


def __dir__():
    return list(globals()) + __all__


__all__ = [*_LAZY, 'SearchConfig', 'Secret', 'search_source_inventory', 'FetchPolicy', 'HttpSource', 'CosSource']
