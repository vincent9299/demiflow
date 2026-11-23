"""Actual pre-layout source carriers retain their original v3 identities and bytes."""
import base64
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
from types import SimpleNamespace

import pytest

from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.recovery_identity import describe
from demiflow.execution.seal.program_pickle import load_payload, load_private_payload

REPO = Path(__file__).resolve().parents[1]
BASE = '3459559152ac51c6aa80d91b7b9c1f5f8ee5eee7'
CARRIERS = {
    'legacy_queue': ['LegacyQueueHistoryDatasource'],
    'dataflow_inputs': ['InputMaterializedHandle'],
    'dataflow_acquisition_call': ['QueueSourceDeclaration', 'AcquisitionCall'],
    'literal_source': ['LiteralSourceReference'],
    'contracts.pipeline': ['PipelineBundleRef', 'PipelineExecutionEnvironment'],
    'requirements': ['FrozenRequirements', 'ResolvedWheel', 'PythonEnvironmentResolution'],
}


@pytest.fixture(scope='module')
def original_carriers(tmp_path_factory):
    original = tmp_path_factory.mktemp('original-layout-carriers')
    archive = original / 'source.tar'
    with archive.open('wb') as output:
        subprocess.run(['git', 'archive', BASE, 'demiflow'], cwd=REPO, stdout=output, check=True)
    with tarfile.open(archive) as source:
        source.extractall(original, filter='data')
    script = '''import base64, importlib, json, pickle, sys
from pathlib import Path
import demiflow
from demiflow.execution.recovery_identity import describe
assert Path(demiflow.__file__).resolve().parent.parent == Path(sys.argv[1])
result = {}
for module, names in json.loads(sys.argv[2]).items():
    imported = importlib.import_module('demiflow.execution.' + module)
    for name in names:
        cls = getattr(imported, name)
        result[name] = [base64.b64encode(pickle.dumps(cls, protocol=5)).decode(), describe(cls)]
import io
from demiflow.execution.legacy_queue import LegacyQueueHistoryDatasource
from demiflow.execution.dataflow_program_pickle import ProgramPickler
source = LegacyQueueHistoryDatasource('/fixed/archive.sqlite', sha256='0'*64, pool='p', through=0)
for key, value in [('legacy_read_task', source.get_read_tasks(1)[0]), ('legacy_rows', source.rows)]:
    buffer = io.BytesIO(); ProgramPickler(buffer, protocol=5).dump(value)
    result[key] = base64.b64encode(buffer.getvalue()).decode()
from demiflow import data
from demiflow.data.guards import require
for fn in [data.from_items, data.read_legacy_queue_records, require]:
    result['public.' + fn.__name__] = [base64.b64encode(pickle.dumps(fn, protocol=5)).decode(), describe(fn)]
print(json.dumps(result))
'''
    child = subprocess.run([sys.executable, '-P', '-B', '-c', script, str(original), json.dumps(CARRIERS)],
                           cwd=original, env={**os.environ, 'PYTHONPATH': str(original)},
                           text=True, capture_output=True, check=True)
    return json.loads(child.stdout)


class OriginalStore:
    def __init__(self, raw):
        self.raw = raw

    def read_into(self, ref, buffer):
        buffer[:] = self.raw


@pytest.mark.parametrize('name', [name for names in CARRIERS.values() for name in names])
@pytest.mark.parametrize('private', [False, True])
def test_published_source_carrier_decodes_with_same_identity(original_carriers, name, private):
    encoded, expected = original_carriers[name]
    raw = base64.b64decode(encoded)
    ref = SimpleNamespace(size_bytes=len(raw), digest=hashlib.sha256(raw).hexdigest())
    loaded = load_private_payload(OriginalStore(raw), ref) if private else load_payload([raw[:5], raw[5:]])
    assert describe(loaded) == expected
    # Loading old bytes never installs an old public module alias.
    if expected['module'] not in {'demiflow.execution.contracts.pipeline', 'demiflow.execution.requirements'}:
        assert expected['module'] not in sys.modules


def test_changed_source_method_cannot_impersonate_published_carrier(original_carriers, monkeypatch):
    from demiflow.execution.sources.legacy_queue import LegacyQueueHistoryDatasource
    method = LegacyQueueHistoryDatasource.rows
    monkeypatch.setattr(method, '__code__', method.__code__.replace(co_consts=(*method.__code__.co_consts, 'changed')))
    raw = base64.b64decode(original_carriers['LegacyQueueHistoryDatasource'][0])
    with pytest.raises(DataflowError, match='audited definition'):
        load_payload([raw])


def test_private_payload_verifies_bytes_before_resolving_original_global(original_carriers):
    raw = base64.b64decode(original_carriers['AcquisitionCall'][0])
    ref = SimpleNamespace(size_bytes=len(raw), digest='0' * 64)
    with pytest.raises(DataflowError, match='digest differs'):
        load_private_payload(OriginalStore(raw), ref)


def test_optimized_reader_accepts_measured_source_carrier(original_carriers):
    raw = original_carriers['InputMaterializedHandle'][0]
    child = subprocess.run([sys.executable, '-OO', '-P', '-B', '-c',
        'import base64,sys; from demiflow.execution.seal.program_pickle import load_payload; '
        'print(load_payload([base64.b64decode(sys.argv[1])]).__name__)', raw],
        env={**os.environ, 'PYTHONPATH': str(REPO)}, text=True, capture_output=True, check=True)
    assert child.stdout.strip() == 'InputMaterializedHandle'


def test_same_method_code_with_unrelated_globals_is_not_authenticated(original_carriers, monkeypatch):
    from types import FunctionType
    from demiflow.execution.sources.legacy_queue import LegacyQueueHistoryDatasource
    method = LegacyQueueHistoryDatasource.rows
    copied = FunctionType(method.__code__, dict(method.__globals__), method.__name__, method.__defaults__)
    copied.__qualname__ = method.__qualname__
    monkeypatch.setattr(LegacyQueueHistoryDatasource, 'rows', copied)
    with pytest.raises(DataflowError, match='audited definition'):
        load_payload([base64.b64decode(original_carriers['LegacyQueueHistoryDatasource'][0])])


def test_source_properties_are_authenticated(original_carriers, monkeypatch):
    from demiflow.execution.sources.materialized_input import InputMaterializedHandle
    monkeypatch.setattr(InputMaterializedHandle, 'blocks', property(lambda self: ()))
    with pytest.raises(DataflowError, match='audited definition'):
        load_payload([base64.b64decode(original_carriers['InputMaterializedHandle'][0])])


def test_private_large_unicode_does_not_duplicate_program_buffer():
    import pickle
    import tracemalloc
    value = 'a' * (4 << 20)
    raw = pickle.dumps(value, protocol=5)
    ref = SimpleNamespace(size_bytes=len(raw), digest=hashlib.sha256(raw).hexdigest())
    tracemalloc.start()
    try:
        loaded = load_private_payload(OriginalStore(raw), ref)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert loaded == value
    assert peak < len(raw) * 2.5


@pytest.mark.parametrize('name', ['legacy_read_task', 'legacy_rows'])
def test_original_source_bound_method_global(original_carriers, name):
    from demiflow.execution.sources.legacy_queue import LegacyQueueHistoryDatasource
    loaded = load_payload([base64.b64decode(original_carriers[name])])
    method = loaded.read_fn if name == 'legacy_read_task' else loaded
    assert method.__func__ is LegacyQueueHistoryDatasource.rows
    assert method.__self__.path == '/fixed/archive.sqlite'


@pytest.mark.parametrize('name', ['from_items', 'read_legacy_queue_records', 'require'])
def test_public_function_import_changes_keep_published_pin(original_carriers, name):
    raw, original = original_carriers['public.' + name]
    assert describe(load_payload([base64.b64decode(raw)])) == original
