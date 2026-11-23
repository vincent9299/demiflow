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
    'run_context': ['RunContext', 'create_target_absent_binding'],
    'checked': ['CheckedExecution'],
    'managed_native': ['NativeExecution'],
    'run_journal': ['RunJournal'],
    'worker.authority': ['WorkerAuthority'],
    'dataflow_source_context': ['source_run_scope'],
    'plan_interpreter': ['PlanContractError'],
    'adaptive_requests': ['AdaptiveRequestGate'],
    'blocking_io': ['BlockingIOPool'],
    'inference_options': ['normalize_call_options'],
    'payload_limits': ['native_row_limit', 'run_row_limit', 'resolve_payload_bounds',
        'batch_target', 'active_limits', 'stream_limits', 'payload_scope', 'arrow_batches'],
    'runtime_limits': ['declared_limit_keys', 'execution_limits'],
    'publication_facts': ['PublicationMismatch', 'PreparedPublicationUnresolved',
        'action_recovery_facts', 'admit_prepared_stage', 'commit_write_context',
        'inspect_write_receipt', 'object_namespace', 'observation_action',
        'observation_context', 'verify_published_output', 'verify_write_receipt'],
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
from demiflow.operator_llm.stream_recovery import RunPromptJournal, NativePromptRecovery, native_request_budget, _BorrowedJournal, _ProtectedPayload, _CapacityPending
from demiflow.embeddings.stream_recovery import NativeEmbeddingRecovery
for fn in [RunPromptJournal, NativePromptRecovery, native_request_budget, NativeEmbeddingRecovery, _BorrowedJournal, _ProtectedPayload, _CapacityPending, RunPromptJournal.check_recovery, RunPromptJournal.media_references, NativePromptRecovery.delivery_trace, NativePromptRecovery.bind_client]:
    result['public.' + fn.__qualname__] = [base64.b64encode(pickle.dumps(fn, protocol=5)).decode(), describe(fn)]
from demiflow.execution.checked import CheckedExecution
for name in ('bind_observation_scope', 'progress', 'rows', 'duration', 'group_duration',
             'actor_created', 'actor_lifecycle', 'worker_actor_lifecycle', 'flush_observations'):
    fn = vars(CheckedExecution)[name]
    result['observation.' + name] = [base64.b64encode(pickle.dumps(fn, protocol=5)).decode(), describe(fn)]
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
    if loaded.__module__ != expected['module']:
        prior = sys.modules.get(expected['module'])
        if prior is not None:
            # A partially split owner may still contain other real definitions.
            # It must be that actual file, never an alias to the moved owner.
            expected_file = REPO / (expected['module'].replace('.', '/') + '.py')
            assert expected_file.is_file()
            assert Path(prior.__file__).resolve() == expected_file
            assert prior is not sys.modules[loaded.__module__]
        else:
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


@pytest.mark.parametrize('name', ['RunPromptJournal', 'NativePromptRecovery', 'native_request_budget',
    'NativeEmbeddingRecovery', '_BorrowedJournal', '_ProtectedPayload', '_CapacityPending',
    'RunPromptJournal.check_recovery', 'RunPromptJournal.media_references',
    'NativePromptRecovery.delivery_trace', 'NativePromptRecovery.bind_client'])
def test_original_model_journal_capture_remains_exact(original_carriers, name):
    raw, expected = original_carriers['public.' + name]
    assert describe(load_payload([base64.b64decode(raw)])) == expected


@pytest.mark.parametrize('module,name', [
    ('demiflow.execution.outputs.object_sink', 'ObjectSink.reference_name'),
    ('demiflow.execution.outputs.object_sink', 'ObjectSink._record_object'),
    ('demiflow.execution.outputs.object_sink', 'ObjectSink._flush_page'),
    ('demiflow.execution.storage.object_resources', 'object_name'),
    ('demiflow.execution.storage.object_resources', 'verify_object'),
])
def test_original_model_journal_requires_actual_media_operation(original_carriers, monkeypatch, module, name):
    import importlib
    value = importlib.import_module(module)
    for part in name.split('.'):
        value = vars(value)[part]
    monkeypatch.setattr(value, '__code__', value.__code__.replace(co_consts=(*value.__code__.co_consts, 'changed')))
    raw, expected = original_carriers['public.RunPromptJournal']
    with pytest.raises(DataflowError, match='audited definition'):
        load_payload([base64.b64decode(raw)])


def test_original_model_journal_requires_bound_media_verifier(original_carriers, monkeypatch):
    from demiflow.execution.outputs import object_sink
    monkeypatch.setattr(object_sink, 'verify_object', lambda *args, **kwargs: None)
    raw, expected = original_carriers['public.RunPromptJournal']
    with pytest.raises(DataflowError, match='audited definition'):
        load_payload([base64.b64decode(raw)])


@pytest.mark.parametrize('descriptor', [property, staticmethod, classmethod])
@pytest.mark.parametrize('name', ['reference_name', '_record_object', '_flush_page'])
def test_original_model_journal_requires_plain_method_slot(original_carriers, monkeypatch, descriptor, name):
    from demiflow.execution.outputs.object_sink import ObjectSink
    method = vars(ObjectSink)[name]
    monkeypatch.setattr(ObjectSink, name, descriptor(method))
    raw, expected = original_carriers['public.RunPromptJournal']
    with pytest.raises(DataflowError, match='audited definition'):
        load_payload([base64.b64decode(raw)])


@pytest.mark.parametrize('name,helper', [
    ('verify_write_receipt', 'verify_target_write_receipt'),
    ('verify_published_output', 'verify_target_stage'),
    ('inspect_write_receipt', 'inspect_prepared_write'),
])
def test_original_publication_requires_actual_target_validator(original_carriers, monkeypatch, name, helper):
    from demiflow.execution.outputs import lance_sink
    function = vars(lance_sink)[helper]
    monkeypatch.setattr(function, '__code__', function.__code__.replace(co_consts=(*function.__code__.co_consts, 'changed')))
    with pytest.raises(DataflowError, match='audited definition'):
        load_payload([base64.b64decode(original_carriers[name][0])])


@pytest.mark.parametrize('change', ['property', 'staticmethod', 'classmethod', 'body', 'closure'])
def test_original_recovery_fact_requires_real_borrowed_connection(original_carriers, monkeypatch, change):
    from demiflow.execution.journal.run_reader import _ReadonlyJournal
    wrapper = vars(_ReadonlyJournal)['_connection']
    if change in ('property', 'staticmethod', 'classmethod'):
        import builtins
        monkeypatch.setattr(_ReadonlyJournal, '_connection', getattr(builtins, change)(wrapper))
    elif change == 'body':
        original = vars(wrapper)['__wrapped__']
        monkeypatch.setattr(original, '__code__', original.__code__.replace(co_consts=(*original.__code__.co_consts, 'changed')))
    else:
        import contextlib
        @contextlib.contextmanager
        def foreign(self):
            yield None
        monkeypatch.setattr(foreign, '__wrapped__', vars(wrapper)['__wrapped__'])
        monkeypatch.setattr(_ReadonlyJournal, '_connection', foreign)
    with pytest.raises(DataflowError, match='audited definition'):
        load_payload([base64.b64decode(original_carriers['action_recovery_facts'][0])])


def test_changed_unlink_cannot_impersonate_original_run_journal(original_carriers, monkeypatch):
    from demiflow.execution.storage.gc import unlink_unprotected_artifact
    monkeypatch.setattr(unlink_unprotected_artifact, '__code__',
        unlink_unprotected_artifact.__code__.replace(
            co_consts=(*unlink_unprotected_artifact.__code__.co_consts, 'changed-unlink')))
    from demiflow.execution.run_journal import RunJournal
    from demiflow.execution.recovery_sdk_v3 import authentic_source_global
    assert not authentic_source_global(RunJournal, 'demiflow.execution.run_journal')
    assert describe(RunJournal) != original_carriers['RunJournal'][1]


@pytest.mark.parametrize('name', ['native_delivery_trace', 'native_response', '_stable_metadata'])
def test_old_prompt_binding_requires_actual_response_projection(original_carriers, monkeypatch, name):
    from demiflow.operator_llm import stream_recovery
    fn = getattr(stream_recovery, name)
    monkeypatch.setattr(fn, '__code__', fn.__code__.replace(co_consts=(*fn.__code__.co_consts, 'changed')))
    with pytest.raises(DataflowError, match='audited definition'):
        load_payload([base64.b64decode(original_carriers['public.NativePromptRecovery'][0])])


def test_old_prompt_binding_requires_bound_response_projection(original_carriers, monkeypatch):
    from demiflow.execution.operator_bindings import dataflow_model_journal
    monkeypatch.setattr(dataflow_model_journal, 'native_response', lambda response, **kw: response)
    with pytest.raises(DataflowError, match='audited definition'):
        load_payload([base64.b64decode(original_carriers['public.NativePromptRecovery.bind_client'][0])])


@pytest.mark.parametrize('name', ['run_scope', 'action_scope', 'persisting_effect_scope', 'is_persisting_effect'])
def test_original_worker_authority_requires_actual_context_owner(original_carriers, monkeypatch, name):
    from demiflow.execution import run_context
    from demiflow.execution.worker.authority import WorkerAuthority
    from demiflow.execution.recovery_sdk_v3 import authentic_source_global
    fn = getattr(run_context, name)
    body = getattr(fn, '__wrapped__', fn)
    monkeypatch.setattr(body, '__code__', body.__code__.replace(co_consts=(*body.__code__.co_consts, 'changed')))
    assert not authentic_source_global(WorkerAuthority, 'demiflow.execution.worker.authority')
    assert describe(WorkerAuthority) != original_carriers['WorkerAuthority'][1]


@pytest.mark.parametrize('name', ['bind_observation_scope', 'progress', 'rows', 'duration',
    'group_duration', 'actor_created', 'actor_lifecycle', 'worker_actor_lifecycle', 'flush_observations'])
def test_original_observation_method_resolves_actual_owner(original_carriers, name):
    from demiflow.execution.checked import CheckedExecution
    from demiflow.execution.journal import dataflow_operator_observations as observations
    raw, expected = original_carriers['observation.' + name]
    loaded = load_payload([base64.b64decode(raw)])
    assert loaded is vars(observations)[name] is vars(CheckedExecution)[name]
    assert describe(loaded) == expected


@pytest.mark.parametrize('mutation', ['slot', 'property', 'staticmethod', 'owner',
    'foreign_globals', 'defaults', 'code'])
def test_original_checked_class_rejects_observation_slot_substitution(original_carriers, monkeypatch, mutation):
    from types import FunctionType
    from demiflow.execution.checked import CheckedExecution
    from demiflow.execution.journal import dataflow_operator_observations as observations
    from demiflow.execution.recovery_sdk_v3 import authentic_source_global
    fn = observations.progress
    if mutation == 'slot':
        monkeypatch.setattr(CheckedExecution, 'progress', lambda *args: None)
    elif mutation == 'property':
        def forbidden_getter(self):
            raise AssertionError('Identity validation must not invoke descriptors')
        monkeypatch.setattr(CheckedExecution, 'progress', property(forbidden_getter))
    elif mutation == 'staticmethod':
        monkeypatch.setattr(CheckedExecution, 'progress', staticmethod(fn))
    elif mutation == 'owner':
        monkeypatch.setattr(observations, 'progress', lambda *args: None)
    elif mutation == 'foreign_globals':
        substitute = FunctionType(fn.__code__, dict(fn.__globals__), fn.__name__, fn.__defaults__)
        monkeypatch.setattr(CheckedExecution, 'progress', substitute)
        monkeypatch.setattr(observations, 'progress', substitute)
    elif mutation == 'defaults':
        monkeypatch.setattr(fn, '__defaults__', (None,))
    else:
        monkeypatch.setattr(fn, '__code__', fn.__code__.replace(co_consts=(*fn.__code__.co_consts, 'changed')))
    assert not authentic_source_global(CheckedExecution, 'demiflow.execution.checked')
    assert describe(CheckedExecution) != original_carriers['CheckedExecution'][1]
