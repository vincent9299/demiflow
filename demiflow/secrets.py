"""Lazy secret references; values never participate in execution identities."""
from dataclasses import dataclass
import os
import re


@dataclass(frozen=True)
class Secret:
    env: str

    def __post_init__(self):
        if type(self.env) is not str or not re.fullmatch(r'[A-Za-z_][A-Za-z_0-9]*', self.env):
            raise ValueError('Invalid secret environment variable name')

    def resolve(self):
        value = os.environ.get(self.env)
        if not value:
            raise ValueError('Missing secret environment variable: ' + self.env)
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


__all__ = ['Secret']
from .collect.fetch_flow import declare_value as _declare_fetch_value
_declare_fetch_value(Secret,'secret')
