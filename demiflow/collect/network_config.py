"""Persistent network settings, read as data rather than executed as shell code."""
from dataclasses import dataclass, field
from pathlib import Path
import os
import re


def load_env(path, *, environ=None):
    values = {}
    path = Path(path)
    if path.exists():
        for number, line in enumerate(path.read_text().splitlines(), 1):
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            key, sep, value = line.partition('=')
            if not sep or not re.fullmatch('[A-Za-z_][A-Za-z0-9_]*', key.strip()):
                raise ValueError(f'Invalid env assignment on line {number}')
            values[key.strip()] = value.strip()
    return {**values, **(dict(os.environ) if environ is None else environ)}


@dataclass(frozen=True)
class NetworkConfig:
    proxy_mode: str = 'direct'
    proxy_url: str | None = None
    host_map: dict = field(default_factory=dict)

    def __post_init__(self):
        if self.proxy_mode not in {'direct', 'environment', 'explicit'}:
            raise ValueError('proxy_mode must be direct, environment or explicit')
        if self.proxy_mode == 'explicit' and not self.proxy_url:
            raise ValueError('explicit proxy requires proxy_url')
        for source, target in self.host_map.items():
            if not source or not target or any(c in source + target for c in '/?#@'):
                raise ValueError('host_map contains an invalid hostname')

    @classmethod
    def from_env(cls, path, *, prefix='DEMIFLOW_', environ=None):
        import json
        values = load_env(path, environ=environ)
        return cls(values.get(prefix + 'PROXY_MODE', 'direct'), values.get(prefix + 'PROXY_URL'),
                   json.loads(values.get(prefix + 'HOST_MAP', '{}')))

    def environment(self, base=None):
        env = dict(os.environ if base is None else base)
        if self.proxy_mode != 'environment':
            env = {key: value for key, value in env.items() if key.lower() not in
                   {'http_proxy', 'https_proxy', 'all_proxy', 'no_proxy'}}
        if self.proxy_mode == 'explicit':
            env.update({key: self.proxy_url for key in ('HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy')})
        return env

    def host(self, hostname):
        return self.host_map.get(hostname, hostname)
