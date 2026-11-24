"""Load search source adapters inside the private worker.

A source is one entry of ``SearchConfig.source_configs()``: a catalog entry
with the caller's overrides. Its adapter is a module, either built in under
``native_search/engines`` (``engine`` names it) or a file the parent resolved
for an installed custom ``module`` or a platform adapter (``engine`` is then a
path without ``.py``).

Each source gets its own module instance, so two sources built on the same
adapter keep separate options. Source settings become module attributes and
missing common attributes get the defaults below. A public module attribute
left at ``None`` is a required option the source did not provide.
"""
import copy
import importlib.util
from pathlib import Path
import re

ENGINES = Path(__file__).with_name('engines')
DEFAULTS = {'engine_type': 'online', 'paging': False, 'max_page': 0, 'time_range_support': False,
            'safesearch': False, 'language_support': False, 'categories': ['general'], 'language': '',
            'region': '', 'enable_http': False, 'shortcut': '-', 'display_error_messages': True,
            'disabled': False, 'inactive': False, 'about': {}, 'using_tor_proxy': False,
            'send_accept_language_header': True, 'tokens': [], 'weight': 1.0}
CAPABILITIES = ('paging', 'max_page', 'time_range_support', 'safesearch', 'language_support', 'language',
                'engine_type')
ENGINE_TYPES = ('online', 'offline')
TAB_CATEGORIES = ('general', 'images', 'videos', 'news', 'map', 'music', 'it', 'science', 'files',
                  'social media')


class SourceLoadError(Exception):
    """The source cannot be used; ``status`` is its receipt status."""

    def __init__(self, status, reason=''):
        super().__init__(reason or status)
        self.status = status


def adapter_file(engine):
    if not isinstance(engine, str) or not engine:
        raise SourceLoadError('configuration_error', 'Source has no adapter')
    if engine.startswith('/'):
        return Path(engine + '.py')
    if not re.fullmatch(r'[a-zA-Z0-9_-]+', engine):
        raise SourceLoadError('configuration_error', 'Invalid adapter name')
    return ENGINES / (engine + '.py')


def _module(path, name):
    spec = importlib.util.spec_from_file_location('demiflow_search_source_' + re.sub(r'\W', '_', name), path)
    if spec is None or spec.loader is None:
        raise SourceLoadError('configuration_error', 'Adapter file not found')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load(settings, timeout_s):
    """Return the adapter module configured for one source, or raise SourceLoadError."""
    name = settings.get('name')
    if not isinstance(name, str) or not name or '_' in name or name != name.lower():
        raise SourceLoadError('configuration_error', 'Invalid source name')
    try:
        module = _module(adapter_file(settings.get('engine')), name)
    except SourceLoadError:
        raise
    except (ImportError, SyntaxError, SystemExit, SystemError, RuntimeError):
        raise SourceLoadError('dependency_error') from None
    except FileNotFoundError:
        raise SourceLoadError('configuration_error', 'Adapter file not found') from None
    except Exception:
        raise SourceLoadError('configuration_error') from None
    about = getattr(module, 'about', None)
    module.about = {**(about if isinstance(about, dict) else {}), **(settings.get('about') or {})}
    for key, value in settings.items():
        if key == 'about':
            continue
        if key == 'categories' and isinstance(value, str):
            value = [part.strip() for part in value.split(',')]
        setattr(module, key, value)
    for key, value in DEFAULTS.items():
        if not hasattr(module, key):
            setattr(module, key, copy.deepcopy(value))
    if not hasattr(module, 'timeout'):
        module.timeout = timeout_s
    if module.inactive is True:
        raise SourceLoadError('configuration_error', 'Source is inactive')
    missing = [k for k in dir(module) if not k.startswith('_') and getattr(module, k) is None]
    if missing:
        raise SourceLoadError('configuration_error', 'Missing source options: ' + ', '.join(sorted(missing)))
    if module.engine_type not in ENGINE_TYPES:
        raise SourceLoadError('unsupported_processor')
    setup = getattr(module, 'setup', None)
    if setup is not None:
        if not callable(setup):
            raise SourceLoadError('configuration_error', 'Source setup is not callable')
        try:
            accepted = setup(settings)
        except Exception:
            raise SourceLoadError('configuration_error', 'Source setup failed') from None
        # Returning nothing means the settings were accepted; only an explicit false value declines.
        if accepted is not None and not accepted:
            raise SourceLoadError('configuration_error', 'Source setup declined')
    if not any(category in TAB_CATEGORIES for category in module.categories):
        module.categories.append('other')
    return module


def transport_defaults(settings):
    """Attributes of a source definition that is not loaded for search: the adapter file's own values
    with ``settings`` laid over them. They describe a network other sources name (``worker.source_networks``)."""
    return {**vars(_module(adapter_file(settings.get('engine')), settings['name'])), **settings}


def capabilities(module):
    return {k: getattr(module, k, None) for k in CAPABILITIES}
