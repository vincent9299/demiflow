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
    'dataflow_api': ['_native_operation_factory'],
    'dataflow_handles': ['RuntimeActionHandle', 'StoredMaterializedHandle', 'TerminalSubscription'],
    'stream': ['StreamStats'],
    'dataflow_operator_observations': ['_ObservedTiming'],
    'legacy_queue': ['LegacyQueueHistoryDatasource'],
    'dataflow_inputs': ['InputMaterializedHandle'],
    'dataflow_acquisition_call': ['QueueSourceDeclaration', 'AcquisitionCall'],
    'literal_source': ['LiteralSourceReference'],
    'contracts.pipeline': ['PipelineBundleRef', 'PipelineExecutionEnvironment', 'PipelineRunRequest'],
    'executors.base': ['DatasetExecutor'],
    'source_readers': ['LanceReader'],
    'requirements': ['FrozenRequirements', 'ResolvedWheel', 'PythonEnvironmentResolution'],
    'request_limits': ['ObservedQueue', 'LatencySummary'],
    'dataset_commit': ['DatasetCommit', 'UncertainDatasetCommit'],
    'target': ['PipelineExecutionTarget'],
    'package_loader': ['BundlePackages'],
    'run_context': ['RunContext'],
    'checked': ['CheckedExecution'],
    'managed_native': ['NativeExecution'],
    'run_journal': ['RunJournal'],
    'adaptive_requests': ['AdaptiveRequestGate'],
    'blocking_io': ['BlockingIOPool'],
    'inference_options': ['normalize_call_options'],
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
from demiflow.data.result_cache import selected
for fn in [data.from_items, data.read_legacy_queue_records, require, selected]:
    result['public.' + fn.__name__] = [base64.b64encode(pickle.dumps(fn, protocol=5)).decode(), describe(fn)]
from demiflow.operator_llm import required_environment, parse_prompt_pack
from demiflow.execution.executors.base import DatasetExecutor
from demiflow.execution.run_reader import read_run
from demiflow.execution.dataflow_capacity import local_capacity_domain
for fn in [data.local_execution, required_environment, parse_prompt_pack, read_run, local_capacity_domain, DatasetExecutor.prepare_input]:
    result['public.' + fn.__name__] = [base64.b64encode(pickle.dumps(fn, protocol=5)).decode(), describe(fn)]
from demiflow import Dataset
from demiflow.inference import request_admission_config
from demiflow.execution.blocking_io import BlockingIOPool
for fn in [Dataset.map_embeddings, request_admission_config, BlockingIOPool.run, BlockingIOPool.drain, BlockingIOPool.aclose]:
    name = fn.__qualname__ if fn.__qualname__.startswith('BlockingIOPool.') else fn.__name__
    result['public.' + name] = [base64.b64encode(pickle.dumps(fn, protocol=5)).decode(), describe(fn)]
from demiflow.execution.dataflow_offline import RemoteOfflinePromptClient
result['offline_class_global'] = base64.b64encode(pickle.dumps(RemoteOfflinePromptClient, protocol=5)).decode()
result['public.Dataset'] = [base64.b64encode(pickle.dumps(Dataset, protocol=5)).decode(), describe(Dataset)]
from demiflow.data.result_cache import CachedMap, CachedSyncMap
for fn in [CachedMap.__call__, CachedMap._io, CachedSyncMap.__call__, CachedSyncMap.__init__]:
    result['public.' + fn.__qualname__] = [base64.b64encode(pickle.dumps(fn, protocol=5)).decode(), describe(fn)]
from demiflow import PipelineBundleRef
buffer = io.BytesIO(); ProgramPickler(buffer, protocol=5).dump(PipelineBundleRef.load)
result['bundle_loader'] = base64.b64encode(buffer.getvalue()).decode()
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
    if loaded.__module__ != expected['module'] and expected['module'] not in {'demiflow.execution.contracts.pipeline', 'demiflow.execution.requirements'}:
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


@pytest.mark.parametrize('name', ['from_items', 'read_legacy_queue_records', 'require',
    'local_execution', 'required_environment', 'parse_prompt_pack', 'read_run', 'local_capacity_domain', 'prepare_input'])
def test_public_function_import_changes_keep_published_pin(original_carriers, name):
    raw, original = original_carriers['public.' + name]
    assert describe(load_payload([base64.b64decode(raw)])) == original


@pytest.mark.parametrize('module,name,carrier', [
    ('demiflow.execution.executors.base', 'prepare_public_items', 'DatasetExecutor'),
    ('demiflow.execution.sources.materialized_input', 'build_input_handle', 'DatasetExecutor'),
    ('demiflow.execution.contracts.record_codec', '_window', 'LanceReader'),
    ('demiflow.execution.contracts.record_codec', '_resident_budget', 'LanceReader'),
])
def test_paired_boundary_requires_actual_helper(original_carriers, monkeypatch, module, name, carrier):
    import importlib
    owner = importlib.import_module(module)
    original = getattr(owner, name)
    # The public class has not changed: a change to its implementation helper
    # must still invalidate the explicitly reviewed old representation.
    monkeypatch.setattr(original, '__code__', original.__code__.replace(co_consts=(*original.__code__.co_consts, 'changed')))
    raw, expected = original_carriers[carrier]
    if carrier == 'LanceReader':
        with pytest.raises(DataflowError, match='audited definition'):
            load_payload([base64.b64decode(raw)])
    else:
        assert describe(load_payload([base64.b64decode(raw)])) != expected


def test_paired_input_requires_import_binding(original_carriers, monkeypatch):
    from demiflow.execution.executors import base
    monkeypatch.setattr(base, 'build_input_handle', lambda *args, **kwargs: None)
    raw, expected = original_carriers['DatasetExecutor']
    assert describe(load_payload([base64.b64decode(raw)])) != expected


def test_original_public_bound_bundle_loader_executes(original_carriers, tmp_path):
    package = tmp_path / 'pipeline'
    package.mkdir()
    (package / '__init__.py').write_text('')
    (package / 'work.py').write_text("from demiurge.demiflow import PipelineProgram\nclass Example(PipelineProgram):\n    execution = {'mode': 'portable'}\n    def run(self, ctx):\n        pass\n")
    loader = load_payload([base64.b64decode(original_carriers['bundle_loader'])])
    ref = loader(tmp_path)
    assert ref.verify() == tmp_path.resolve()
    assert ref.module_key == 'p_' + ref.content_digest.removeprefix('sha256:')[:24]


@pytest.mark.parametrize('private', [False, True])
def test_published_whole_dataset_class_keeps_all_audited_method_pins(original_carriers, private):
    encoded, expected = original_carriers['public.Dataset']
    raw = base64.b64decode(encoded)
    ref = SimpleNamespace(size_bytes=len(raw), digest=hashlib.sha256(raw).hexdigest())
    loaded = load_private_payload(OriginalStore(raw), ref) if private else load_payload([raw])
    assert describe(loaded) == expected


@pytest.mark.parametrize("name", ["map_prompt_async", "map_embeddings"])
def test_changed_dataset_method_is_not_normalized_to_old_code(original_carriers, monkeypatch, name):
    from demiflow import Dataset
    method = getattr(Dataset, name)
    before = describe(Dataset)
    monkeypatch.setattr(method, '__code__', method.__code__.replace(co_consts=(*method.__code__.co_consts, 'changed')))
    after = describe(Dataset)
    assert after != before


def test_relocated_queue_class_keeps_budgets_and_order(original_carriers):
    raw = base64.b64decode(original_carriers['ObservedQueue'][0])
    queue = load_payload([raw])(2, max_item_bytes=4096, max_bytes=8192)
    queue.put_nowait({'id': 1})
    queue.put_nowait({'id': 2})
    assert queue.full()
    assert [queue.get_nowait(), queue.get_nowait()] == [{'id': 1}, {'id': 2}]
    assert queue.bytes == 0 and queue.wait.count == 2


def test_relocated_queue_method_rejects_foreign_class_cell(original_carriers, monkeypatch):
    from types import FunctionType, CellType
    from demiflow.execution.worker.stream_resources import ObservedQueue
    method = ObservedQueue.full
    wrong = FunctionType(method.__code__, method.__globals__, method.__name__, method.__defaults__, (CellType(object),))
    wrong.__module__, wrong.__qualname__ = method.__module__, method.__qualname__
    monkeypatch.setattr(ObservedQueue, 'full', wrong)
    raw = base64.b64decode(original_carriers['ObservedQueue'][0])
    with pytest.raises(DataflowError, match='audited definition'):
        load_payload([raw])


def test_published_unchanged_cache_callback_keeps_measured_identity(original_carriers):
    raw, expected = original_carriers['public.selected']
    selected = load_payload([base64.b64decode(raw)])
    assert describe(selected) == expected
    assert selected(lambda row: row['id'] > 0, {'id': 1}) is True
    assert selected(lambda row: row['id'] > 0, {'id': -1}) is False


def test_relocated_context_wrapper_authenticates_original_closure(original_carriers, monkeypatch):
    from demiflow.execution.journal.run_reader import read_run
    monkeypatch.setattr(read_run, '__wrapped__', lambda *a, **k: None)
    raw = base64.b64decode(original_carriers['public.read_run'][0])
    with pytest.raises(DataflowError, match='audited definition'):
        load_payload([raw])


@pytest.mark.parametrize('change', ['wrapped', 'globals', 'body'])
def test_relocated_async_context_wrapper_rejects_forged_function(original_carriers, monkeypatch, change):
    from types import FunctionType
    from demiflow.services.adaptive_requests import AdaptiveRequestGate
    method = AdaptiveRequestGate.enter
    if change == 'wrapped':
        monkeypatch.setattr(method, '__wrapped__', lambda: None)
    elif change == 'globals':
        forged = FunctionType(method.__code__, dict(method.__globals__), method.__name__,
                              method.__defaults__, method.__closure__)
        forged.__dict__.update(method.__dict__)
        forged.__qualname__, forged.__module__ = method.__qualname__, method.__module__
        monkeypatch.setattr(AdaptiveRequestGate, 'enter', forged)
    else:
        original = method.__wrapped__
        monkeypatch.setattr(original, '__code__', original.__code__.replace(
            co_consts=(*original.__code__.co_consts, 'not the original admission')))
    with pytest.raises(DataflowError, match='audited definition'):
        load_payload([base64.b64decode(original_carriers['AdaptiveRequestGate'][0])])


def test_old_offline_client_global_reads_actual_algorithm_without_old_module_alias(original_carriers):
    from demiflow.operator_llm.offline import RemoteOfflinePromptClient
    assert load_payload([base64.b64decode(original_carriers['offline_class_global'])]) is RemoteOfflinePromptClient
    assert 'demiflow.execution.dataflow_offline' not in sys.modules


def test_relocated_commit_class_rejects_changed_body(original_carriers, monkeypatch):
    from demiflow.execution.checkpoint.dataset_commit import DatasetCommit
    method = DatasetCommit.confirm
    monkeypatch.setattr(method, '__code__', method.__code__.replace(co_consts=(*method.__code__.co_consts, 'changed')))
    with pytest.raises(DataflowError, match='audited definition'):
        load_payload([base64.b64decode(original_carriers['DatasetCommit'][0])])


@pytest.mark.parametrize('change', ['code', 'globals'])
def test_native_factory_old_global_requires_its_actual_definition(original_carriers, monkeypatch, change):
    from types import FunctionType
    from demiflow.execution.executors import dataflow_api
    fn = dataflow_api._native_operation_factory
    if change == 'code':
        monkeypatch.setattr(fn, '__code__', fn.__code__.replace(co_consts=(*fn.__code__.co_consts, 'changed')))
    else:
        replacement = FunctionType(fn.__code__, dict(fn.__globals__), fn.__name__, fn.__defaults__)
        replacement.__qualname__ = fn.__qualname__
        monkeypatch.setattr(dataflow_api, '_native_operation_factory', replacement)
    raw = base64.b64decode(original_carriers['_native_operation_factory'][0])
    with pytest.raises(DataflowError, match='audited definition'):
        load_payload([raw])


@pytest.mark.parametrize('change', ['code', 'globals', 'class_cell', 'initializer'])
def test_original_protocol_rejects_forged_typing_methods(original_carriers, monkeypatch, change):
    from types import FunctionType
    from demiflow.execution.outputs.dataflow_handles import TerminalSubscription
    hook = vars(TerminalSubscription)['__subclasshook__']
    if change == 'initializer':
        original = TerminalSubscription.__init__
        forged = FunctionType(original.__code__, original.__globals__, original.__name__)
        monkeypatch.setattr(TerminalSubscription, '__init__', forged)
    else:
        code, namespace, closure = hook.__code__, hook.__globals__, hook.__closure__
        if change == 'code':
            code = code.replace(co_consts=(*code.co_consts, 'changed'))
        elif change == 'globals':
            namespace = dict(namespace)
        else:
            other = object()
            closure = (lambda: other).__closure__
        forged = FunctionType(code, namespace, hook.__name__, None, closure)
        monkeypatch.setattr(TerminalSubscription, '__subclasshook__', forged)
    with pytest.raises(DataflowError, match='audited definition'):
        load_payload([base64.b64decode(original_carriers['TerminalSubscription'][0])])


@pytest.mark.parametrize('name', ['map_embeddings', 'request_admission_config',
    'CachedMap.__call__', 'CachedMap._io', 'CachedSyncMap.__call__', 'CachedSyncMap.__init__',
    'BlockingIOPool.run', 'BlockingIOPool.drain', 'BlockingIOPool.aclose'])
@pytest.mark.parametrize('private', [False, True])
def test_original_public_function_keeps_effective_sdk_pin(original_carriers, name, private):
    encoded, expected = original_carriers['public.' + name]
    raw = base64.b64decode(encoded)
    ref = SimpleNamespace(size_bytes=len(raw), digest=hashlib.sha256(raw).hexdigest())
    loaded = load_private_payload(OriginalStore(raw), ref) if private else load_payload([raw])
    assert describe(loaded) == expected


def test_changed_request_policy_cannot_keep_original_pin(original_carriers, monkeypatch):
    from demiflow.inference import request_admission_config
    original = request_admission_config.__code__
    monkeypatch.setattr(request_admission_config, '__code__', original.replace(co_consts=(*original.co_consts, 'changed')))
    assert describe(request_admission_config) != original_carriers['public.request_admission_config'][1]
