"""Qualified provider version registry: single source, env-overridable."""
from __future__ import annotations

import pytest

from demiflow.forge.schema import ForgeError
from demiflow.workbench.conversations import versions


@pytest.fixture(autouse=True)
def isolate_qualification_environment(monkeypatch):
    monkeypatch.delenv('DEMIFLOW_CODEX_VERSIONS', raising=False)
    monkeypatch.delenv('DEMIFLOW_DEEPSEEK_VERSIONS', raising=False)


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


def _codex(version='0.170.0'):
    from demiflow.workbench.conversations.codex import Codex
    return Codex(_Transport(), {'version': version}, {'root': '/tmp'})


def test_adapter_accepts_a_newly_qualified_version(monkeypatch):
    client = _codex()
    monkeypatch.setenv('DEMIFLOW_CODEX_VERSIONS', '0.170.0')
    client.initialized({'userAgent': 'codex-cli/0.170.0'})
    assert client.transport.sent


def test_adapter_still_rejects_unqualified_version(monkeypatch):
    client = _codex()
    monkeypatch.setenv('DEMIFLOW_CODEX_VERSIONS', '0.170.0')
    with pytest.raises(ForgeError, match='qualified version'):
        client.initialized({'userAgent': 'codex-cli/0.150.0'})


@pytest.mark.parametrize('actual', ['0.170.00', '0.170.0-rc.1', '0.170.0+local', '10.170.0'])
def test_qualification_requires_the_entire_version_token(monkeypatch, actual):
    monkeypatch.setenv('DEMIFLOW_CODEX_VERSIONS', '0.170.0')
    client = _codex()
    with pytest.raises(ForgeError, match='qualified version'):
        client.initialized({'userAgent': 'codex-cli/' + actual})
    assert not client.transport.sent


def test_adapter_requires_the_configured_version_even_when_both_are_qualified(monkeypatch):
    monkeypatch.setenv('DEMIFLOW_CODEX_VERSIONS', '0.164.0,0.170.0')
    client = _codex('0.164.0')
    with pytest.raises(ForgeError, match='qualified version'):
        client.initialized({'userAgent': 'codex_cli_rs/0.170.0 (Linux; x86_64)'})
    assert not client.transport.sent


@pytest.mark.parametrize('version', ['0.164.0', '0.170.0'])
def test_adapter_accepts_the_exact_configured_version(monkeypatch, version):
    monkeypatch.setenv('DEMIFLOW_CODEX_VERSIONS', '0.164.0,0.170.0')
    client = _codex(version)
    client.initialized({'userAgent': f'codex_cli_rs/{version} (Linux; x86_64)'})
    assert client.transport.sent
