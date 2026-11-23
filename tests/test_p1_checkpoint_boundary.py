"""Checkpoint read extraction preserves its closed gate and retained core seams.

The checkpoint facade is retired. Tests of the retained read implementation
replace only checked/action admission to inspect historical source semantics;
they are not evidence that public checkpoint execution is supported again.
"""
from contextlib import contextmanager
from types import SimpleNamespace
import threading

import lance
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.data.api import DataAPI
from demiflow.data.dataset import Dataset, MaterializedDataset
from demiflow.data.execution_contract import _use_executor
from demiflow.data.lance_declaration import make_lance_scan_source
from demiflow.data.sources import LanceSource, MaterializedSource
from demiflow.errors import InvalidLanceRequest
from demiflow.execution.executors.stream_checkpoint import read_checkpoint_binding
from demiflow.execution.executors.stream_checkpoint import StreamCheckpoint, read_checkpoint


class _Port:
    def __init__(self, events):
        self.events = events
        self.inputs = []

    def prepare_input(self, request):
        from demiflow.execution.executors.base import DatasetExecutor
        self.events.append('input')
        self.inputs.append(request)
        return DatasetExecutor.prepare_input(self, request)


class _Lock:
    def __init__(self, events):
        self.events = events
        self.inner = threading.RLock()

    def __enter__(self):
        self.inner.acquire()
        self.events.append('lock')

    def __exit__(self, *exc):
        self.events.append('unlock')
        self.inner.release()


def _checkpoint(tmp_path, events, *, version=None):
    """A read-only historical shape; never bypass the real public constructor."""
    checkpoint = object.__new__(StreamCheckpoint)
    schema = pa.schema([
        pa.field('id', pa.int64(), nullable=False, metadata={b'unit': b'identity'}),
        pa.field('name', pa.string()),
    ], metadata={b'dataset': b'checkpoint'})
    uri = str(tmp_path / 'stage.lance')
    checkpoint.declared_outputs = {'stage': {'uri': uri, 'schema': schema}}
    checkpoint.contract_arguments = {'checkpoint': 'original-declaration'}
    checkpoint.state = {
        'format': 2, 'identity': 'a' * 64, 'revision': 0,
        'outputs': {} if version is None else {'stage': {'uri': uri, 'version': version}},
        'pending': None,
    }
    checkpoint.lock = _Lock(events)
    original = checkpoint.validate_ownership
    def validate():
        events.append('ownership')
        original()
    checkpoint.validate_ownership = validate
    return checkpoint


def _checked_seam(monkeypatch, events, *, owner=None):
    from demiflow.execution.pipeline import checked
    registrations, arguments = [], []
    class Checked:
        def checkpoint_read(self, source, reference):
            events.append('register')
            registrations.append((source, reference))
    @contextmanager
    def action(kind, source, plan, *, arguments):
        assert kind == 'read_checkpoint' and source is None and plan is None
        events.append('checked')
        action_arguments.append(arguments)
        yield Checked()
    action_arguments = arguments
    monkeypatch.setattr(checked, 'action', action)
    monkeypatch.setattr(checked, 'current', lambda: owner)
    return registrations, arguments


@pytest.mark.parametrize('columns', [None, ['id']])
def test_empty_binding_preserves_arrow_type_metadata_and_registered_source(tmp_path, monkeypatch, columns):
    events = []
    checkpoint = _checkpoint(tmp_path, events)
    registered, arguments = _checked_seam(monkeypatch, events)
    port = _Port(events)
    binding = read_checkpoint_binding(checkpoint, 'stage', columns=columns, executor=port)
    assert isinstance(binding.source, MaterializedSource)
    assert binding.reference is None and binding.plan.is_empty
    assert registered == [(binding.source, None)]
    assert registered[0][0] is binding.source
    assert events == ['checked', 'lock', 'ownership', 'unlock', 'input', 'register']
    assert arguments == [{'checkpoint': checkpoint.contract_arguments,
                          'stage': 'stage', 'columns': list(columns or ())}]
    request, = port.inputs
    assert request.kind == 'arrow' and request.value.num_rows == 0
    expected = checkpoint.declared_outputs['stage']['schema']
    if columns:
        expected = pa.schema([expected.field(name) for name in columns], metadata=expected.metadata)
    assert request.value.schema.equals(expected, check_metadata=True)
    assert binding.source.handle._value[0].schema.equals(expected, check_metadata=True)
    binding.source.handle.release()


def test_canonical_empty_assembly_keeps_source_type_executor_and_selection_order(tmp_path, monkeypatch):
    import demiflow.data.api as api
    events = []
    checkpoint = _checkpoint(tmp_path, events)
    registered, _ = _checked_seam(monkeypatch, events)
    port = _Port(events)
    def select(executor, **kwargs):
        assert executor is port
        events.append('select')
        return executor
    monkeypatch.setattr(api, '_select_executor', select)
    with _use_executor(port):
        result = read_checkpoint(checkpoint, 'stage', columns=['id'])
    assert type(result) is MaterializedDataset
    assert result._source is registered[0][0] and result._executor is port
    assert events == ['checked', 'lock', 'ownership', 'unlock', 'select', 'input', 'register']
    result._source.handle.release()


def test_fixed_binding_snapshots_reference_once_before_selection_and_registers_same_source(tmp_path, monkeypatch):
    import demiflow.data.api as api
    events, opened = [], []
    checkpoint = _checkpoint(tmp_path, events, version=1)
    registered, _ = _checked_seam(monkeypatch, events)
    port = _Port(events)
    def select(executor, **kwargs):
        events.append('select')
        # Simulate the writer advancing after the original lock was released.
        checkpoint.state['outputs']['stage']['version'] = 2
        return port
    def open_fixed(**reference):
        events.append('schema')
        opened.append(reference)
        return SimpleNamespace(schema=checkpoint.declared_outputs['stage']['schema'])
    monkeypatch.setattr(api, '_select_executor', select)
    monkeypatch.setattr(lance, 'dataset', open_fixed)
    result = read_checkpoint(checkpoint, 'stage', columns=['id'])
    assert type(result) is Dataset and type(result._source) is LanceSource
    assert result._source is registered[0][0]
    assert registered[0][1] == {'uri': checkpoint.declared_outputs['stage']['uri'], 'version': 1}
    assert opened == [registered[0][1]] and port.inputs == []
    assert result._source.query.version == 1
    assert result._source.query.batch_readahead == result._source.query.fragment_readahead == 1
    assert events == ['checked', 'lock', 'ownership', 'unlock', 'select', 'schema', 'register']
    expected = DataAPI(port).read_lance(opened[0]['uri'], version=1, columns=['id'],
                                     batch_readahead=1, fragment_readahead=1)._source
    from demiflow.execution.seal.plan_facts import source_facts
    from demiflow.execution.recovery_identity import source_identity
    assert result._source.query.to_dict() == expected.query.to_dict()
    assert result._source.native_options == expected.native_options
    assert source_facts(result._source) == source_facts(expected)
    assert source_identity(result._source) == source_identity(expected)


@pytest.mark.parametrize('failure', ['wrong_type', 'stage', 'ownership', 'pending'])
def test_invalid_checkpoint_fails_before_default_host_or_lance_access(tmp_path, monkeypatch, failure):
    import demiflow.data.api as api
    events = []
    checkpoint = _checkpoint(tmp_path, events)
    registered, _ = _checked_seam(monkeypatch, events)
    def unexpected(*args, **kwargs):
        raise AssertionError('invalid checkpoint must not construct a host or open a table')
    monkeypatch.setattr(api, '_select_executor', unexpected)
    monkeypatch.setattr(lance, 'dataset', unexpected)
    stage = 'stage'
    if failure == 'wrong_type':
        checkpoint = SimpleNamespace(declared_outputs={})
        message = 'declared native checkpoint'
    elif failure == 'stage':
        stage, message = 'missing', 'absent from declared'
    elif failure == 'ownership':
        checkpoint.state['outputs']['stage'] = {'uri': str(tmp_path / 'foreign.lance'), 'version': 1}
        message = 'declared ownership'
    else:
        checkpoint.state['pending'] = {'kind': 'create', 'stage': 'stage',
            'uri': checkpoint.declared_outputs['stage']['uri'], 'operation_id': 'a' * 32}
        message = 'unresolved commit'
    with pytest.raises(ValueError, match=message):
        read_checkpoint(checkpoint, stage)
    assert registered == []
    if failure in {'wrong_type', 'stage'}:
        assert events == []
    else:
        assert events == ['checked', 'lock', 'ownership', 'unlock']


def test_schema_mismatch_rejects_before_source_registration(tmp_path, monkeypatch):
    events = []
    checkpoint = _checkpoint(tmp_path, events, version=3)
    registered, _ = _checked_seam(monkeypatch, events)
    monkeypatch.setattr(lance, 'dataset', lambda **ref: SimpleNamespace(schema=pa.schema([('id', pa.string())])))
    port = _Port(events)
    with pytest.raises(ValueError, match='schema differs'):
        read_checkpoint_binding(checkpoint, 'stage', executor=port)
    assert registered == [] and port.inputs == []


def test_managed_checkpoint_facts_stay_with_original_checked_owner(tmp_path, monkeypatch):
    events = []
    checkpoint = _checkpoint(tmp_path, events)
    facts = {'managed': 'original-resource'}
    owner = SimpleNamespace(checkpoint_facts=lambda candidate: facts if candidate is checkpoint else None)
    registered, arguments = _checked_seam(monkeypatch, events, owner=owner)
    result = read_checkpoint_binding(checkpoint, 'stage', executor=_Port(events))
    assert arguments[0]['checkpoint'] is facts
    assert registered[0][0] is result.source
    result.source.handle.release()


def test_real_capability_gate_still_rejects_checkpoint_before_host_selection(tmp_path, monkeypatch):
    from demiflow.execution.recovery_contract import RecoveryContractError
    import demiflow.data.api as api
    events = []
    checkpoint = _checkpoint(tmp_path, events)
    monkeypatch.setattr(api, '_select_executor', lambda *_: pytest.fail('retired checkpoint must not select host'))
    with pytest.raises(RecoveryContractError, match='Explicit checkpoint orchestration is retired'):
        data.read_checkpoint(checkpoint, 'stage')
    assert events == []
    assert data.read_checkpoint is read_checkpoint
    assert read_checkpoint.__module__ == 'demiflow.execution.executors.stream_checkpoint'


@pytest.mark.parametrize('projection', [False, True])
@pytest.mark.parametrize('native', [False, True])
def test_public_and_shared_lance_scan_have_identical_declaration_and_facts(tmp_path, projection, native):
    from demiflow.data.native_options import parse_native_options
    from demiflow.execution.seal.plan_facts import source_facts
    from demiflow.execution.recovery_identity import source_identity
    from demiflow.lance.model import LanceScanSpec
    options = dict(uri=str(tmp_path / 'input.lance'), version=4, filter='id >= 0', limit=9,
                   batch_size=2, batch_readahead=3, fragment_readahead=4,
                   storage_options={}, projection={'next_id': 'id + 1'} if projection else None,
                   columns=None if projection else ['id'])
    backend = {'ray': {'worker_resources': {'cpu': 2}}} if native else None
    expected = LanceSource(LanceScanSpec(**options), parse_native_options(backend, family='source'))
    direct = make_lance_scan_source(**options, backend_options=backend)
    public = DataAPI(_Port([])).read_lance(**options, backend_options=backend)._source
    for source in (direct, public):
        assert source.query.to_dict() == expected.query.to_dict()
        assert source.native_options == expected.native_options
        if native:
            from demiflow.data.runtime_contract import RuntimeContractError
            # Managed facts deliberately reject native backend options; the
            # source extraction must preserve this existing rejection domain.
            for candidate in (source, expected):
                with pytest.raises(RuntimeContractError, match='Unknown native scan contract'):
                    source_facts(candidate)
        else:
            assert source_facts(source) == source_facts(expected)
        assert source_identity(source) == source_identity(expected)


def test_lance_scan_validation_precedes_native_options(tmp_path):
    uri = str(tmp_path / 'input.lance')
    for construct in (make_lance_scan_source, DataAPI(_Port([])).read_lance):
        with pytest.raises(InvalidLanceRequest, match='version'):
            construct(uri, version=0, backend_options=object())
        with pytest.raises(ValueError, match='ray mapping'):
            construct(uri, version=1, backend_options=object())
