"""The shared launcher rejects unimplemented isolation before any preparation."""
import json
import os
import sys

import pytest

from demiflow.resources.process_observation import UNIT_ENV
from demiflow.workbench.conversations import execution


@pytest.mark.parametrize('mode', ['restricted', 'unknown', None, {}, True])
def test_unavailable_mode_rejects_before_adapter_preparation_or_spawn(tmp_path, monkeypatch, mode):
    calls = []

    class Adapter:
        @staticmethod
        def launch(*args):
            calls.append('adapter')
            (tmp_path / 'credential-copy').write_text('must not happen')
            raise AssertionError('Unavailable execution prepared a provider')

    def spawn(*args, **kwargs):
        calls.append('spawn')
        raise AssertionError('Unavailable execution spawned a provider')

    monkeypatch.setattr(execution.subprocess, 'Popen', spawn)
    with pytest.raises(ValueError):
        execution.launch_provider(mode=mode, adapter=Adapter,
            config={'qualified': True, 'sandbox': 'restricted'},
            home=tmp_path / 'home', root=tmp_path / 'project', lease_fd=-1)
    assert calls == []
    assert list(tmp_path.iterdir()) == []


def test_capability_projection_cannot_enable_restricted_execution(monkeypatch):
    from demiflow.forge.sandbox import sandbox
    monkeypatch.setattr(sandbox, 'backend_readiness', lambda: dict(available=False, reason='Unavailable',
        backend='fixture', implementation='fixture', identity_model='fixture'))
    facts = execution.execution_facts('restricted')
    facts['available'] = True
    facts['anti_tamper'] = True
    with pytest.raises(ValueError):
        execution.require_available('restricted')
    trusted = execution.execution_facts('trusted_local')
    assert trusted['available'] is True
    assert trusted['anti_tamper'] is False
    assert trusted['filesystem_enforcement'] == 'unsupported'


def test_trusted_local_exec_preserves_stdio_and_only_explicit_lease_fd(tmp_path, monkeypatch):
    monkeypatch.setenv('DEMIFLOW_EXECUTION_TEST_SECRET', 'not-inherited')
    source_env = {'PATH': '/usr/bin:/bin', 'LANG': 'C.UTF-8', 'EXPLICIT': 'kept'}
    with (tmp_path / 'unit').open('w+b') as lease, (tmp_path / 'unrelated').open('w+b') as unrelated:
        os.set_inheritable(unrelated.fileno(), True)
        lease_info = os.fstat(lease.fileno())
        unrelated_info = os.fstat(unrelated.fileno())
        code = """
import json, os, sys
unit = os.fstat(int(os.environ['DEMIFLOW_EXECUTION_UNIT_FD']))
try:
    other = os.fstat(int(sys.argv[1]))
    other_identity = [other.st_dev, other.st_ino]
except OSError:
    other_identity = None
print(json.dumps(dict(unit=[unit.st_dev, unit.st_ino], unrelated=other_identity,
    inherited_secret='DEMIFLOW_EXECUTION_TEST_SECRET' in os.environ,
    explicit=os.environ['EXPLICIT'], pid=os.getpid(), session=os.getsid(0),
    input=sys.stdin.buffer.read().decode(), argument=sys.argv[2])))
sys.stderr.write('provider-stderr')
"""

        class Adapter:
            @staticmethod
            def launch(config, home, root):
                assert config == {'model': 'fake'}
                assert home == root == tmp_path
                return [sys.executable, '-I', '-B', '-c', code,
                        str(unrelated.fileno()), '; echo not-a-shell'], source_env

        process = execution.launch_provider(mode='trusted_local', adapter=Adapter,
            config={'model': 'fake'}, home=tmp_path, root=tmp_path, lease_fd=lease.fileno())
        try:
            stdout, stderr = process.communicate(b'provider-input', timeout=5)
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate(timeout=5)
        assert process.returncode == 0, stderr.decode()
        result = json.loads(stdout)
        assert result['unit'] == [lease_info.st_dev, lease_info.st_ino]
        assert result['unrelated'] != [unrelated_info.st_dev, unrelated_info.st_ino]
        assert result['inherited_secret'] is False
        assert result['explicit'] == 'kept'
        assert result['session'] == result['pid'] == process.pid
        assert result['input'] == 'provider-input'
        assert result['argument'] == '; echo not-a-shell'
        assert stderr == b'provider-stderr'
    assert UNIT_ENV not in source_env


def test_spawn_failure_has_no_retry_or_fallback(tmp_path, monkeypatch):
    calls = []

    class Adapter:
        @staticmethod
        def launch(*args):
            calls.append('adapter')
            return [sys.executable, '-V'], {}

    def fail(*args, **kwargs):
        calls.append('spawn')
        raise OSError('spawn failed')

    monkeypatch.setattr(execution.subprocess, 'Popen', fail)
    with (tmp_path / 'unit').open('w+b') as lease:
        with pytest.raises(OSError, match='spawn failed'):
            execution.launch_provider(mode='trusted_local', adapter=Adapter,
                config={}, home=tmp_path, root=tmp_path, lease_fd=lease.fileno())
    assert calls == ['adapter', 'spawn']
