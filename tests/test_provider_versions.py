"""Qualified provider version registry: single source, env-overridable."""
from __future__ import annotations

import importlib

import pytest

from demiflow.workbench.conversations import versions


def test_defaults_keep_historically_qualified_codex_versions():
    qualified = versions.qualified_versions('codex')
    assert '0.164.0' in qualified
    # 未验收的新版本默认不放行，必须由部署侧显式启用
    assert '0.170.0' not in qualified
    assert versions.is_qualified('codex', '0.164.0')
    assert not versions.is_qualified('codex', '0.170.0')
    assert not versions.is_qualified('codex', '9.9.9')
    assert not versions.is_qualified('codex', None)


def test_environment_override_replaces_whitelist(monkeypatch):
    monkeypatch.setenv('DEMIFLOW_CODEX_VERSIONS', '0.170.0, 0.171.0')
    assert versions.qualified_versions('codex') == ('0.170.0', '0.171.0')
    assert versions.is_qualified('codex', '0.170.0')
    assert not versions.is_qualified('codex', '0.164.0')


def test_blank_environment_override_falls_back_to_defaults(monkeypatch):
    monkeypatch.setenv('DEMIFLOW_CODEX_VERSIONS', ' , ')
    assert versions.qualified_versions('codex') == versions.DEFAULT_VERSIONS['codex']


def test_user_agent_matching_covers_whole_whitelist(monkeypatch):
    assert versions.matches_user_agent('codex', 'codex-cli/0.164.0')
    monkeypatch.setenv('DEMIFLOW_CODEX_VERSIONS', '0.170.0')
    assert versions.matches_user_agent('codex', 'codex-cli/0.170.0')
    assert not versions.matches_user_agent('codex', 'codex-cli/0.164.0')
    assert not versions.matches_user_agent('codex', None)


class _Transport:
    def __init__(self):
        self.sent = []

    def send(self, message):
        self.sent.append(message)

    def request(self, method, params):
        return {'method': method, 'config': {}}


def _codex():
    from demiflow.workbench.conversations.codex import Codex
    return Codex(_Transport(), {}, {'root': '/tmp'})


def test_adapter_accepts_a_newly_qualified_version(monkeypatch):
    client = _codex()
    monkeypatch.setenv('DEMIFLOW_CODEX_VERSIONS', '0.170.0')
    client.initialized({'userAgent': 'codex-cli/0.170.0'})
    assert client.transport.sent


def test_adapter_still_rejects_unqualified_version(monkeypatch):
    from demiflow.workbench.conversations.common import reject
    client = _codex()
    monkeypatch.setenv('DEMIFLOW_CODEX_VERSIONS', '0.170.0')
    with pytest.raises(Exception):
        client.initialized({'userAgent': 'codex-cli/0.150.0'})
