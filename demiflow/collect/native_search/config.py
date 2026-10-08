"""Immutable declarations for the bundled search runtime. No I/O at declaration."""
from __future__ import annotations

from dataclasses import dataclass, field
from functools import lru_cache
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import re
from urllib.parse import urlsplit, parse_qsl

VENDOR = Path(__file__).resolve().parents[2] / '_vendor' / 'searxng'
RUNTIME_VERSION = 'native-search-1'


@dataclass(frozen=True)
class Secret:
    """Resolve an environment variable at execution, never in a config snapshot."""
    env: str

    def __post_init__(self):
        if not re.fullmatch(r'[A-Za-z_][A-Za-z_0-9]*', self.env):
            raise ValueError('Invalid secret environment variable name')

    def resolve(self):
        value = os.environ.get(self.env)
        if not value:
            raise ValueError('Missing search secret: ' + self.env)
        return value


def public(value):
    if isinstance(value, Secret):
        return {'secret_env': value.env}
    if isinstance(value, dict):
        return {k: public(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)):
        return [public(v) for v in value]
    return value


def resolve(value):
    if isinstance(value, Secret):
        return value.resolve()
    if isinstance(value, dict):
        return {k: resolve(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)):
        return [resolve(v) for v in value]
    return value


def validate_language(language):
    if not isinstance(language, str) or not language:
        raise ValueError('Declare search language on the request or SearchConfig')
    if language != 'all':
        import babel
        try:
            babel.Locale.parse(language, sep='-')
        except (ValueError, babel.UnknownLocaleError):
            raise ValueError('Invalid search language') from None
    return language


def validate_secrets(value, key=''):
    if isinstance(value, Secret):
        return
    if isinstance(value, dict):
        for k, v in value.items():
            validate_secrets(v, k)
    elif isinstance(value, (tuple, list)):
        for v in value:
            validate_secrets(v, key)
    elif isinstance(value, str) and value:
        if re.search(r'password|api[_-]?key|secret|token|authorization|cookie|auth', key, re.I):
            raise ValueError('Search credential must use Secret(env=...): ' + key)
        if '://' in value:
            parsed = urlsplit(value)
            if parsed.username or any(re.search(r'^key$|password|api[_-]?key|secret|token|authorization|signature|credential', k, re.I) for k,_ in parse_qsl(parsed.query)):
                raise ValueError('Authenticated search URLs must use Secret(env=...)')


@lru_cache(maxsize=1)
def catalog():
    import yaml
    return yaml.safe_load((VENDOR / 'searx/settings.yml').read_text())


@lru_cache(maxsize=1)
def baseline_id():
    return json.loads((VENDOR / 'BASELINE.json').read_text())['baseline_id']


@lru_cache(maxsize=1)
def runtime_id():
    files = list(Path(__file__).parent.glob('*.py'))
    files.append(Path(__file__).resolve().parents[1] / 'proxy.py')
    files.append(Path(__file__).resolve().parents[1] / 'searxng.py')
    files.append(Path(__file__).resolve().parents[2] / 'services/engines/wikisearch.py')
    from importlib.metadata import version, PackageNotFoundError
    dependencies = {}
    for name in ('curl_cffi', 'babel', 'flask-babel', 'lxml', 'msgspec', 'python-dateutil', 'isodate',
                 'mysql-connector-python', 'mariadb', 'psycopg2-binary', 'pymongo', 'playwright'):
        try:
            dependencies[name] = version(name)
        except PackageNotFoundError:
            dependencies[name] = None
    return digest([RUNTIME_VERSION, baseline_id(), dependencies,
                   [(p.name, hashlib.sha256(p.read_bytes()).hexdigest()) for p in sorted(files)]])


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                     separators=(',', ':')).encode()).hexdigest()


@dataclass(frozen=True)
class SearchConfig:
    """Native retrieval policy; source overrides follow bundled SearXNG settings.

    ``engines`` is an ordered list of catalog names or source dictionaries. A
    custom dictionary may use ``module='installed.package.adapter'``. Secrets
    use Secret, including authenticated proxy URLs. ``networks`` maps request
    network names to outgoing overrides; all are scoped to this session.
    """
    engines: tuple = ('wikisearch', 'wikipedia')
    language: str | None = None
    workers: int = 4
    request_concurrency: int = 4
    host_concurrency: int = 2
    host_interval_s: float = 1.0
    source_interval_s: float = 0.0
    source_concurrency: int = 1
    timeout_s: float = 30.0
    startup_timeout_s: float = 30.0
    retries: int = 0
    retry_delay_s: float = 1.0
    failure_limit: int = 3
    query_failure_limit: int | None = None
    suspend_s: float = 60.0
    max_bytes: int = 4 * 1024 * 1024
    max_results: int = 500
    max_redirects: int = 5
    proxy: Secret | str | dict | None = None
    networks: dict = field(default_factory=dict)
    browser: dict = field(default_factory=dict)

    def __post_init__(self):
        import sys
        if sys.version_info < (3, 11) or os.name != 'posix':
            raise RuntimeError('Native search requires Python 3.11+ on POSIX')
        for name in ('workers', 'request_concurrency', 'host_concurrency', 'source_concurrency', 'failure_limit', 'max_bytes', 'max_results'):
            if type(getattr(self, name)) is not int or getattr(self, name) < 1:
                raise ValueError('Invalid search option: ' + name)
        for name in ('retries', 'max_redirects'):
            if type(getattr(self, name)) is not int or getattr(self, name) < 0:
                raise ValueError('Invalid search option: ' + name)
        if self.query_failure_limit is not None and (type(self.query_failure_limit) is not int or self.query_failure_limit < 1):
            raise ValueError('Invalid search option: query_failure_limit')
        for name in ('host_interval_s', 'source_interval_s', 'timeout_s', 'startup_timeout_s', 'retry_delay_s', 'suspend_s'):
            v = getattr(self, name)
            if type(v) not in (int, float) or not math.isfinite(v) or v < 0 or (name.endswith('timeout_s') and not v):
                raise ValueError('Invalid search option: ' + name)
        if not self.engines:
            raise ValueError('Declare at least one search source')
        if self.language is not None:
            validate_language(self.language)
        validate_secrets(self.engines)
        validate_secrets(self.networks)
        validate_secrets(self.proxy, 'proxy')
        from ..proxy import proxy_declaration
        object.__setattr__(self, 'proxy', proxy_declaration(self.proxy))
        # Copy declarations: a caller mutating its dict cannot alter a running session.
        import copy
        object.__setattr__(self, 'engines', tuple(copy.deepcopy(self.engines)))
        object.__setattr__(self, 'networks', copy.deepcopy(self.networks))
        from .browser import browser_options
        object.__setattr__(self, 'browser', browser_options(self.browser))

    def snapshot(self):
        return public(vars(self))

    @classmethod
    def from_mapping(cls, value):
        """Read a JSON-compatible declaration; Secret values stay unresolved."""
        if not isinstance(value, dict):
            raise TypeError('Search configuration must be a mapping')
        def declaration(item):
            if isinstance(item, dict):
                if set(item) == {'secret_env'}:
                    return Secret(item['secret_env'])
                return {k: declaration(v) for k, v in item.items()}
            if isinstance(item, (list, tuple)):
                return [declaration(v) for v in item]
            return item
        return cls(**declaration(value))

    def source_configs(self):
        import copy
        defaults = {c['name'].lower(): c for c in catalog()['engines']}
        defaults['wikisearch'] = {'name': 'wikisearch', 'engine': 'wikisearch', 'categories': ['general']}
        result = []
        names = set()
        for entry in self.engines:
            overrides = {'name': entry} if isinstance(entry, str) else dict(entry)
            name = overrides.get('name')
            if isinstance(name, str):
                name = name.lower()
                overrides['name'] = name
            if not isinstance(name, str) or not name or '_' in name or name in names:
                raise ValueError('Search source names must be unique lowercase names without underscores')
            names.add(name)
            if name not in defaults and not (overrides.get('engine') or overrides.get('module')):
                raise ValueError('Unknown search source: ' + name)
            config = {**copy.deepcopy(defaults.get(name, {})), **overrides}
            if 'backend' in config:
                if config['backend'] not in ('http', 'browser') or config.get('engine') not in {'google','google_images'}:
                    raise ValueError('Explicit http/browser backends support Google web and images')
                config.setdefault('categories', ['images'] if config['engine']=='google_images' else ['general', 'web'])
            config.setdefault('shortcut', name)
            if 'categories' in config and isinstance(config['categories'], str):
                config['categories'] = [c.strip() for c in config['categories'].split(',')]
            if 'weight' in config and (type(config['weight']) not in (int, float) or not math.isfinite(config['weight']) or config['weight'] <= 0):
                raise ValueError('Source weight must be finite and positive')
            if 'timeout' in config and (type(config['timeout']) not in (int, float) or not math.isfinite(config['timeout']) or config['timeout'] <= 0):
                raise ValueError('Source timeout must be finite and positive')
            config['disabled'] = False  # selection is explicit, not browser preferences
            if config.get('inactive'):
                raise ValueError('Source is inactive; explicitly override inactive to enable: ' + name)
            if config.get('module'):
                module = config.pop('module')
                if not isinstance(config.get('revision'), str) or not config['revision']:
                    raise ValueError('Custom search adapters require a revision covering their dependencies')
                spec = importlib.util.find_spec(module)
                if not spec or not spec.origin or not spec.origin.endswith('.py'):
                    raise ValueError('Custom search adapter must be an installed Python module: ' + module)
                config['engine'] = spec.origin[:-3]
                config['_demiflow_module'] = module
                config['_demiflow_module_sha256'] = hashlib.sha256(Path(spec.origin).read_bytes()).hexdigest()
            elif config.get('engine') == 'wikisearch':
                config['engine'] = str(Path(__file__).resolve().parents[2] / 'services/engines/wikisearch')
            else:
                engine = config.get('engine', '')
                if not re.fullmatch(r'[a-zA-Z0-9_-]+', engine) or not (VENDOR / 'searx/engines' / (engine + '.py')).is_file():
                    raise ValueError('Unknown bundled search adapter; use module for installed custom sources')
            result.append(config)
        return result


def search_source_inventory():
    """Return bundled capabilities and independent validation states; no network."""
    return json.loads(Path(__file__).with_name('inventory.json').read_text())
