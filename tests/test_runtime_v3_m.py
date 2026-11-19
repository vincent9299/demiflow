"""Lane M unit gate G-M: models, state, cache and native journal adaptation.

Production symbols under test live in this worktree. The C-lane DTOs
(``demiflow.execution.contracts.dataflow`` / ``dataflow_errors``) are owned by
lane C and are not merged into this tree, so this module installs strict-contract
*test-domain doubles* into ``sys.modules`` before importing the production M
modules. The doubles mirror the frozen ``interfaces.pyi`` field lists and the
real C validation (closed error codes, sha256 operation ids, bounded idents,
reservation shape, UNKNOWN_EFFECT never retryable). They are marked
``__test_double__`` and carry a synthetic ``__file__`` so evidence can separate
real module bindings from doubles. The driver-side journal/effect ledger is also
a test-domain double; only loopback HTTP fixtures stand in for external services.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import sys
import threading
import types
from dataclasses import dataclass, field, fields, is_dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import MappingProxyType
from typing import Mapping, Protocol

import pytest
from test_runtime_v3_c import make as make_contract
from demiflow.execution.contracts.dataflow import thaw_record_value

# --- in-process worktree assertion (IL012) ----------------------------------
TREE = Path(__file__).resolve().parents[1]
import demiflow
assert Path(demiflow.__file__).resolve().is_relative_to(TREE), (
    'demiflow must import from this worktree, got ' + str(demiflow.__file__))


# --- strict-contract C-lane test-domain doubles -----------------------------
_ERROR_CODES = frozenset({
    'INVALID_CONTRACT', 'UNSUPPORTED_CAPABILITY', 'RESOURCE_INFEASIBLE',
    'BUDGET_EXCEEDED', 'SOURCE_CHANGED', 'CODE_MISMATCH', 'FENCED', 'CANCELLED',
    'DEADLINE_EXCEEDED', 'CORRUPT_ARTIFACT', 'UNKNOWN_EFFECT',
    'INCOMPATIBLE_HISTORY', 'INTERNAL_PROTOCOL',
})
_SHA256 = __import__('re').compile(r'[0-9a-f]{64}\Z')
_IDENT = __import__('re').compile(r'[A-Za-z0-9_.:\-]{1,128}\Z')


def _real_module_available(name):
    import importlib
    try:
        importlib.import_module(name)
    except ImportError:
        return False
    return True


def _install_contract_doubles():
    import importlib
    if _real_module_available("demiflow.execution.contracts.dataflow"):
        return (importlib.import_module("demiflow.execution.contracts.dataflow"),
                importlib.import_module("demiflow.execution.contracts.dataflow_errors"))
    errors = types.ModuleType('demiflow.execution.contracts.dataflow_errors')
    errors.__file__ = '<test-double:demiflow.execution.contracts.dataflow_errors>'
    errors.__test_double__ = True

    class DataflowError(Exception):
        def __init__(self, code, message, *, retryable=False, context=None):
            if type(code) is not str or code not in _ERROR_CODES:
                raise ValueError('unknown dataflow error code: ' + repr(code))
            if type(message) is not str or not 1 <= len(message) <= 2048:
                raise ValueError('error message must be a bounded nonempty string')
            if type(retryable) is not bool:
                raise ValueError('retryable must be a bool')
            if code == 'UNKNOWN_EFFECT' and retryable:
                raise ValueError('UNKNOWN_EFFECT must not be retryable')
            self.code, self.retryable, self.context = code, retryable, context
            super().__init__(code + ': ' + message)

    errors.DataflowError = DataflowError
    errors.DATAFLOW_ERROR_CODES = _ERROR_CODES

    df = types.ModuleType('demiflow.execution.contracts.dataflow')
    df.__file__ = '<test-double:demiflow.execution.contracts.dataflow>'
    df.__test_double__ = True

    def err(message):
        return DataflowError('INVALID_CONTRACT', message)

    def sha(value, label):
        if type(value) is not str or _SHA256.fullmatch(value) is None:
            raise err(label + ' must be lowercase sha256 hex')
        return value

    def ident(value, label):
        if type(value) is not str or _IDENT.fullmatch(value) is None:
            raise err(label + ' must be a bounded identifier')
        return value

    def enum(value, label, allowed):
        if type(value) is not str or value not in allowed:
            raise err(label + ' must be one of ' + ','.join(sorted(allowed)))
        return value

    def freeze_json(value, label):
        if value is None or type(value) in (bool, int, str):
            return value
        if type(value) is float:
            import math
            if not math.isfinite(value):
                raise err(label + ' must be finite')
            return value
        if type(value) is dict:
            return MappingProxyType({k: freeze_json(v, label) for k, v in value.items()})
        if type(value) in (list, tuple):
            return tuple(freeze_json(v, label) for v in value)
        raise err(label + ' must be declarative JSON')

    def json_map(value, label):
        if not isinstance(value, Mapping):
            raise err(label + ' must be a mapping')
        return MappingProxyType({ident(k, label + ' key'): freeze_json(v, label)
                                 for k, v in value.items()})

    @dataclass(frozen=True)
    class ArtifactRef:
        digest: str
        size_bytes: int
        store_id: str
        key: str
        media_type: str

    @dataclass(frozen=True)
    class Bounds:
        max_nodes: int
        max_edges: int
        max_partitions: int
        max_depth: int
        max_control_bytes: int
        max_row_bytes: int
        max_batch_bytes: int
        max_resident_bytes: int
        max_spill_bytes: int
        max_state_bytes: int
        max_pending_events: int
        max_open_files: int
        max_output_rows: int
        timeout_ms: int

        def __post_init__(self):
            for f in fields(self):
                v = getattr(self, f.name)
                if type(v) is not int or v < 1:
                    raise err('Bounds.' + f.name + ' must be an integer >= 1')

    @dataclass(frozen=True)
    class InputPort:
        name: str
        requirement: str
        ordering: str
        schema_ref: object

        def __post_init__(self):
            enum(self.requirement, 'InputPort.requirement', {'incremental', 'finite_complete'})
            enum(self.ordering, 'InputPort.ordering',
                 {'none', 'source_partition', 'key_sequence', 'global'})

    from demiflow.resources.contracts import ResourceProfile

    @dataclass(frozen=True)
    class KernelSpec:
        kind: str
        implementation_ref: object
        configuration: Mapping
        inputs: tuple
        state: str
        effect: str
        resource_role: str
        profile: ResourceProfile
        output_bound_bytes: int
        mergeable: bool

        def __post_init__(self):
            ident(self.kind, 'KernelSpec.kind')
            object.__setattr__(self, 'configuration', json_map(self.configuration, 'KernelSpec.configuration'))
            enum(self.state, 'KernelSpec.state', {'none', 'auto_json', 'adapter', 'nonrecoverable'})
            enum(self.effect, 'KernelSpec.effect', {'pure', 'journaled', 'unknown_replay_forbidden'})
            enum(self.resource_role, 'KernelSpec.resource_role', {'cpu', 'gpu', 'remote_api', 'io'})
            if not isinstance(self.profile, ResourceProfile):
                raise err('KernelSpec.profile must be a ResourceProfile')
            if type(self.output_bound_bytes) is not int or self.output_bound_bytes < 0:
                raise err('KernelSpec.output_bound_bytes must be a nonnegative integer')
            if type(self.mergeable) is not bool:
                raise err('KernelSpec.mergeable must be a bool')

    @dataclass(frozen=True)
    class Record:
        record_id: str
        source_position: Mapping
        payload_ref: object
        inline: object
        encoded_bytes: int
        sequence: int

    @dataclass(frozen=True)
    class EffectCommand:
        operation_id: str
        kind: str
        effect_id: str
        actor_id: str
        request_digest: str
        payload_ref: object
        budget: Mapping

        def __post_init__(self):
            sha(self.operation_id, 'EffectCommand.operation_id')
            enum(self.kind, 'EffectCommand.kind',
                 {'lookup', 'reserve', 'mark_sent', 'confirm', 'unknown', 'release_unsent'})
            ident(self.effect_id, 'EffectCommand.effect_id')
            ident(self.actor_id, 'EffectCommand.actor_id')
            sha(self.request_digest, 'EffectCommand.request_digest')

    @dataclass(frozen=True)
    class EffectReply:
        operation_id: str
        status: str
        result_ref: object
        receipt_ref: object

        def __post_init__(self):
            enum(self.status, 'EffectReply.status',
                 {'absent', 'reserved', 'sent', 'confirmed', 'unknown', 'unsent'})

    @dataclass(frozen=True)
    class NativeJournalCommand:
        operation_id: str
        operation: str
        action_id: str
        node_id: str
        request_key: str
        request_ref: object
        value_ref: object
        reservation: object
        options: Mapping

        def __post_init__(self):
            sha(self.operation_id, 'NativeJournalCommand.operation_id')
            enum(self.operation, 'NativeJournalCommand.operation',
                 {'lookup', 'reserve', 'reserve_http_retry', 'response', 'failed',
                  'references', 'media_references', 'bind_membership'})
            ident(self.action_id, 'NativeJournalCommand.action_id')
            ident(self.node_id, 'NativeJournalCommand.node_id')
            if type(self.request_key) is not str or not 1 <= len(self.request_key) <= 512:
                raise err('NativeJournalCommand.request_key must be bounded')
            if not isinstance(self.request_ref, ArtifactRef):
                raise err('NativeJournalCommand.request_ref must be an ArtifactRef')
            if self.reservation is not None:
                rid, charges = self.reservation
                ident(rid, 'reservation id')
                frozen = {}
                for key, amount in charges.items():
                    ident(key, 'reservation charge key')
                    a, c = amount
                    if type(a) is not int or a < 0 or type(c) is not int or c < 0:
                        raise err('reservation charge must be nonnegative (amount, limit)')
                    frozen[key] = (a, c)
                object.__setattr__(self, 'reservation', (rid, MappingProxyType(frozen)))
            object.__setattr__(self, 'options', json_map(self.options, 'NativeJournalCommand.options'))

    @dataclass(frozen=True)
    class NativeJournalReply:
        operation_id: str
        value: object
        value_ref: object

    @dataclass(frozen=True)
    class NativeRunView:
        limits: Mapping
        fence: str
        check_stop: object

        def __post_init__(self):
            object.__setattr__(self, 'limits', json_map(self.limits, 'NativeRunView.limits'))
            ident(self.fence, 'NativeRunView.fence')

    @dataclass(frozen=True)
    class NativeActionView:
        action_id: str
        context: NativeRunView
        object_sinks: Mapping
        native_image_effects: bool

        def __post_init__(self):
            ident(self.action_id, 'NativeActionView.action_id')
            if not isinstance(self.context, NativeRunView):
                raise err('NativeActionView.context must be a NativeRunView')
            object.__setattr__(self, 'object_sinks', MappingProxyType(dict(self.object_sinks)))
            if type(self.native_image_effects) is not bool:
                raise err('NativeActionView.native_image_effects must be a bool')

    @dataclass(frozen=True)
    class WorkerAssignment:
        # Test-domain projection: production reads action_id/fence only. The
        # nested lease/sealed/stage DTOs are lane C owned and not exercised here.
        schema: str = 'demiflow.dataflow_assignment.v3'
        run_id: str = 'run-1'
        action_id: str = 'action-1'
        attempt: int = 1
        fence: str = 'fence-1'
        plan_id: str = 'plan-1'
        plan_revision: int = 0
        lease: object = None
        sealed: object = None
        stages: tuple = ()
        inputs: tuple = ()
        outputs: tuple = ()
        source_splits: tuple = ()
        restore_refs: tuple = ()
        bounds: object = None

    @dataclass(frozen=True)
    class LoadedProgram:
        kind: str
        value: object
        digest: str

    @dataclass(frozen=True)
    class RuntimeServices:
        store: object
        journal: object
        bounds: Bounds
        assignment: WorkerAssignment
        clock_ms: object
        check_stop: object
        constants: Mapping
        programs: Mapping

        def __post_init__(self):
            if not isinstance(self.bounds, Bounds):
                raise err('RuntimeServices.bounds must be a Bounds')
            if not isinstance(self.assignment, WorkerAssignment):
                raise err('RuntimeServices.assignment must be a WorkerAssignment')
            object.__setattr__(self, 'constants', json_map(self.constants, 'RuntimeServices.constants'))
            object.__setattr__(self, 'programs', MappingProxyType(dict(self.programs)))

    class ArtifactStore(Protocol):
        def put(self, chunks, *, max_bytes, media_type): ...
        def read(self, ref, *, max_bytes): ...
        def inspect(self, ref): ...
        def retain(self, ref, *, owner): ...
        def release(self, ref, *, owner): ...

    class Kernel(Protocol):
        def start(self): ...
        def push(self, port, record): ...
        def finish(self, port): ...
        def snapshot(self): ...
        def restore(self, ref): ...
        def close(self): ...

    class WorkerJournalPort(Protocol):
        def native_request(self, command): ...
        def queue(self, command): ...
        def effect(self, command): ...
        def artifact(self, command): ...
        def reserve(self, operation_id, amounts): ...
        def read_action(self, action_id): ...
        def read_configuration(self): ...
        def check_grant(self): ...

    class NativeEffectTransport(Protocol):
        def lookup(self, command): ...
        def submit(self, command): ...

    class CallableInstance(Protocol):
        def __call__(self, row): ...

    for name, value in dict(
        ArtifactRef=ArtifactRef, Bounds=Bounds, InputPort=InputPort, KernelSpec=KernelSpec,
        Record=Record, EffectCommand=EffectCommand, EffectReply=EffectReply,
        NativeJournalCommand=NativeJournalCommand, NativeJournalReply=NativeJournalReply,
        NativeRunView=NativeRunView, NativeActionView=NativeActionView,
        WorkerAssignment=WorkerAssignment, LoadedProgram=LoadedProgram,
        RuntimeServices=RuntimeServices, ArtifactStore=ArtifactStore, Kernel=Kernel,
        WorkerJournalPort=WorkerJournalPort, NativeEffectTransport=NativeEffectTransport,
        CallableInstance=CallableInstance,
    ).items():
        setattr(df, name, value)

    import demiflow.execution.contracts as contracts_pkg
    sys.modules[errors.__name__] = errors
    sys.modules[df.__name__] = df
    contracts_pkg.dataflow_errors = errors
    contracts_pkg.dataflow = df
    return df, errors


DTO, ERRORS = _install_contract_doubles()
ArtifactRef = DTO.ArtifactRef
Bounds = DTO.Bounds
InputPort = DTO.InputPort
KernelSpec = DTO.KernelSpec
Record = DTO.Record
EffectCommand = DTO.EffectCommand
EffectReply = DTO.EffectReply
NativeJournalCommand = DTO.NativeJournalCommand
NativeJournalReply = DTO.NativeJournalReply
NativeRunView = DTO.NativeRunView
NativeActionView = DTO.NativeActionView
WorkerAssignment = DTO.WorkerAssignment
LoadedProgram = DTO.LoadedProgram
RuntimeServices = DTO.RuntimeServices
DataflowError = ERRORS.DataflowError

# --- production symbols under test (imported AFTER doubles are installed) ----
from demiflow.execution import dataflow_models as models
from demiflow.execution import dataflow_model_journal as journal
from demiflow.execution.recovery_contract import identity, RecoveryContractError
from demiflow.operator_llm.journal import RequestRecord, request_key, canonical
from demiflow.operator_llm.stream_recovery import (
    CALL_RESERVATION, native_request_budget, NativePromptRecovery, _BorrowedJournal,
)
from demiflow.embeddings.stream_recovery import NativeEmbeddingRecovery
from demiflow.operator_llm.runtime import (
    PromptActor, InProcessOperatorLLMCoordinator, AsyncOperatorLLMRuntime,
)
from demiflow.operator_llm.client import AsyncOperatorLLMClient, create_async_operator_llm_client
from demiflow.operator_llm.parser import parse_prompt_pack
from demiflow.data.plan import (
    OperatorLLMMapOp, EmbeddingMapOp, AgentMapOp, SearchWebOp, VectorSearchOp,
    ReadDocumentsOp, MapOp, CallableSpec,
)
from demiflow.embeddings import EmbeddingModel
from demiflow.embeddings.runtime import EmbeddingActor
from demiflow.execution.blocking_io import BlockingIOPool
from demiflow.execution.adaptive_requests import AdaptiveRequestGate
from demiflow.execution.row_scope import RowScope, set_row_scope
from demiflow.resources.contracts import ResourceProfile
from demiflow.environment import OperatorEnvironment

RemotePromptJournal = journal.RemotePromptJournal
RemotePromptRecovery = journal.RemotePromptRecovery
RemoteEmbeddingRecovery = journal.RemoteEmbeddingRecovery


# --- test-domain infrastructure ---------------------------------------------
def make_bounds(**overrides):
    values = dict(max_nodes=64, max_edges=128, max_partitions=8, max_depth=16,
                  max_control_bytes=16 * 1024**2, max_row_bytes=8 * 1024**2,
                  max_batch_bytes=8 * 1024**2, max_resident_bytes=64 * 1024**2,
                  max_spill_bytes=64 * 1024**2, max_state_bytes=1024**2,
                  max_pending_events=1024, max_open_files=256,
                  max_output_rows=1_000_000, timeout_ms=30_000)
    values.update(overrides)
    return Bounds(**values)


def _thaw(value):
    if isinstance(value, Mapping):
        return {k: _thaw(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_thaw(v) for v in value]
    return value


class FakeStore:
    """In-memory ArtifactStore double; production owns the byte-budget checks."""
    def __init__(self):
        self._data = {}
        self._owners = {}

    def put(self, chunks, *, max_bytes, media_type):
        data = b''.join(chunks)
        if len(data) > max_bytes:
            raise DataflowError('BUDGET_EXCEEDED', 'artifact exceeds max_bytes')
        digest = hashlib.sha256(data).hexdigest()
        self._data[digest] = data
        return ArtifactRef(digest=digest, size_bytes=len(data), store_id='test-store',
                           key=digest, media_type=media_type)

    def read(self, ref, *, max_bytes):
        yield self._data[ref.digest]

    def inspect(self, ref):
        return ref.digest in self._data

    def retain(self, ref, *, owner):
        self._owners.setdefault(ref.digest, set()).add(owner)

    def release(self, ref, *, owner):
        self._owners.get(ref.digest, set()).discard(owner)


def _ref_dict(key, kind, attempt, journal_path='driver-native-journal'):
    value = {'journal_path': journal_path, 'request_id': key, 'kind': kind}
    if attempt != 1:
        value['attempt'] = attempt
    return value


class FakeDriverJournal:
    """Driver-side native journal/effect ledger double (C09 state machine)."""
    def __init__(self, store, *, limits=None):
        self.store = store
        self.limits = dict(limits or {'max_requests': 100, 'max_control_bytes': 16 * 1024**2})
        self.requests = {}
        self.effects = {}
        self.membership = {}
        self.budgets = {}
        self.reservations = {}
        self.commands = []
        self.effect_commands = []
        self.reserve_calls = []
        self.closed = False

    def _read(self, ref):
        return json.loads(b''.join(self.store.read(ref, max_bytes=16 * 1024**2)))

    def _references(self, key, attempt):
        return {'request_id': key, 'journal_path': 'driver-native-journal',
                'request_ref': _ref_dict(key, 'request', attempt),
                'response_ref': _ref_dict(key, 'response', attempt)}

    def read_action(self, action_id):
        return {'limits': dict(self.limits)}

    def read_configuration(self):
        return {}

    def check_grant(self):
        return None

    def reserve(self, operation_id, amounts):
        self.reserve_calls.append((operation_id, dict(amounts)))
        for name, amount in amounts.items():
            self.budgets[name] = self.budgets.get(name, 0) + amount

    def queue(self, command):
        raise DataflowError('UNSUPPORTED_CAPABILITY', 'queue not used by lane M')

    def artifact(self, command):
        raise DataflowError('UNSUPPORTED_CAPABILITY', 'artifact not used by lane M')

    def native_request(self, command):
        self.commands.append(command)
        op, key = command.operation, command.request_key
        if op == 'lookup':
            entry = self.requests.get(key)
            if entry is None:
                return NativeJournalReply(command.operation_id, None, None)
            if entry['status'] in ('reserved', 'unknown', 'failed'):
                raise DataflowError('UNKNOWN_EFFECT',
                                    'pending/unknown request is not a cache miss')
            value = entry['response']
            if command.options.get('with_references'):
                value = {'response': value, 'references': self._references(key, entry['attempt'])}
            return NativeJournalReply(command.operation_id, value, None)
        if op == 'reserve':
            if command.reservation is None:
                raise DataflowError('INVALID_CONTRACT', 'reserve requires a CALL_RESERVATION budget')
            rid, charges = command.reservation
            for name, (amount, ceiling) in charges.items():
                if self.budgets.get(name, 0) + amount > ceiling:
                    raise DataflowError('BUDGET_EXCEEDED', 'request budget exhausted: ' + name)
            entry = self.requests.get(key)
            if entry is not None and entry['status'] == 'confirmed':
                return NativeJournalReply(command.operation_id, False, None)
            for name, (amount, ceiling) in charges.items():
                self.budgets[name] = self.budgets.get(name, 0) + amount
            self.reservations[rid] = charges
            self.requests[key] = {'status': 'reserved', 'response': None, 'error': None,
                                  'attempt': 1, 'media': ()}
            return NativeJournalReply(command.operation_id, True, None)
        if op == 'reserve_http_retry':
            if command.reservation is None:
                raise DataflowError('INVALID_CONTRACT', 'HTTP retry requires a budget')
            entry = self.requests.get(key)
            opts = command.options
            # a complete saved HTTP error response (not an uncertain call) is retryable
            if (entry is None or entry['status'] != 'confirmed'
                    or entry['response'] is None
                    or entry['response'].get('status_code') != opts['expected_status']
                    or entry['attempt'] != opts['expected_attempt']):
                raise DataflowError('UNKNOWN_EFFECT', 'HTTP retry no longer matches the saved complete error')
            if entry['attempt'] >= opts['max_attempts']:
                raise DataflowError('BUDGET_EXCEEDED', 'per-request HTTP attempt limit exhausted')
            entry['attempt'] += 1
            entry.update(status='reserved', response=None, error=None)
            return NativeJournalReply(command.operation_id, True, None)
        if op == 'response':
            record = self._read(command.value_ref)
            entry = self.requests.setdefault(key, {'attempt': 1, 'media': ()})
            entry.update(status='confirmed', response=record, error=None)
            refs = self._references(key, entry['attempt'])
            return NativeJournalReply(command.operation_id, refs, None)
        if op == 'failed':
            payload = self._read(command.value_ref)
            entry = self.requests.setdefault(key, {'attempt': 1, 'media': ()})
            call = payload.get('call') or {}
            unsent = call.get('request_sent') is False and call.get('batch_cancelled_before_dispatch') is True
            if unsent:
                status = 'unsent'
            elif type(call.get('http_status')) is int:
                status = 'failed'  # a complete saved HTTP error is retryable
            else:
                status = 'unknown'  # timeout/connection loss is never auto-unsent
            entry.update(status=status, error=payload, error_status=call.get('http_status'))
            return NativeJournalReply(command.operation_id, None, None)
        if op == 'references':
            entry = self.requests.get(key)
            attempt = entry['attempt'] if entry else 1
            return NativeJournalReply(command.operation_id, self._references(key, attempt), None)
        if op == 'media_references':
            entry = self.requests.get(key)
            if entry is None:
                raise DataflowError('CORRUPT_ARTIFACT', 'media reference has no confirmed response')
            names = entry.get('media') or ()
            if command.options.get('register') and not names:
                raise DataflowError('CORRUPT_ARTIFACT', 'cached image lost its owned object')
            return NativeJournalReply(command.operation_id, list(names), None)
        if op == 'bind_membership':
            mkey = command.options['membership_key']
            rk = command.options['request_key']
            if mkey in self.membership and self.membership[mkey] != rk:
                raise DataflowError('INCOMPATIBLE_HISTORY', 'batch membership changed for occurrence')
            self.membership[mkey] = rk
            return NativeJournalReply(command.operation_id, None, None)
        raise DataflowError('UNSUPPORTED_CAPABILITY', 'unknown native operation: ' + op)

    def effect(self, command):
        self.effect_commands.append(command)
        eid = command.effect_id
        entry = self.effects.get(eid)
        if entry is not None and entry['request_digest'] != command.request_digest:
            raise DataflowError('INVALID_CONTRACT', 'effect request_digest mismatch')
        kind = command.kind
        if kind == 'lookup':
            if entry is None:
                return EffectReply(command.operation_id, 'absent', None, None)
            return EffectReply(command.operation_id, entry['status'],
                               entry.get('result_ref'), entry.get('receipt_ref'))
        if kind == 'reserve':
            if entry is not None and entry['status'] in ('sent', 'unknown', 'confirmed'):
                raise DataflowError('UNKNOWN_EFFECT', 'cannot re-reserve a sent/unknown effect')
            self.effects[eid] = {'status': 'reserved', 'request_digest': command.request_digest}
            return EffectReply(command.operation_id, 'reserved', None, None)
        if kind == 'mark_sent':
            self.effects[eid]['status'] = 'sent'
            return EffectReply(command.operation_id, 'sent', None, None)
        if kind == 'confirm':
            self.effects[eid].update(status='confirmed', result_ref=command.payload_ref)
            return EffectReply(command.operation_id, 'confirmed', command.payload_ref, None)
        if kind == 'unknown':
            self.effects[eid]['status'] = 'unknown'
            return EffectReply(command.operation_id, 'unknown', None, None)
        if kind == 'release_unsent':
            self.effects[eid]['status'] = 'unsent'
            return EffectReply(command.operation_id, 'unsent', None, None)
        raise DataflowError('UNSUPPORTED_CAPABILITY', 'unknown effect kind: ' + kind)


def make_services(*, limits=None, programs=None, action_id='action-1', fence='fence-1'):
    store = FakeStore()
    port = FakeDriverJournal(store, limits=limits)
    services = RuntimeServices(
        store=store, journal=port, bounds=make_bounds(),
        assignment=make_contract("WorkerAssignment", action_id=action_id, fence=fence, bounds=make_bounds()),
        clock_ms=lambda: 0, check_stop=lambda: None, constants={},
        programs=programs or {})
    return services, store, port


def make_journal(services, node_id='node-1'):
    action = journal.make_native_action_view(services, node_id=node_id)
    remote = RemotePromptJournal(port=services.journal, action=action, bounds=services.bounds)
    remote._bind(node_id, services.store)
    return remote, action


def req(body=None):
    return RequestRecord({'protocol': 'openai_compatible', 'model_contract': {'name': 'm'},
                          'body': body if body is not None else {'x': 1}})


def effect_command(kind, effect_id, digest, *, payload_ref=None, operation_id=None):
    return EffectCommand(operation_id=operation_id or identity(['effect', kind, effect_id]),
                         kind=kind, effect_id=effect_id, actor_id='node-1',
                         request_digest=digest, payload_ref=payload_ref, budget={})


PACK = '''
schema_version: demiflow_prompt_pack_v2
prompts:
  enrich:
    version: v1
    model:
      name: mock-model
      transport: openai_compatible
      base_url_env: TEST_M_PROMPT_URL
      api_key_env: TEST_M_PROMPT_KEY
    schema_retries: 0
    response_schema:
      type: object
      additionalProperties: false
      required: [result]
      properties:
        result: {type: string}
    template: |
      {{ payload | json }}
'''


@pytest.fixture
def prompt_server(monkeypatch):
    state = {'requests': [], 'status': 200, 'fail_first': 0, 'answer': {'result': 'ok'}}
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            with lock:
                index = len(state['requests'])
                state['requests'].append(body)
                status = state['status']
                if index < state['fail_first']:
                    status = 500
            if status == 200:
                payload = json.dumps({'choices': [{'message': {'content': json.dumps(state['answer'])}}],
                                      'usage': {'prompt_tokens': 10, 'completion_tokens': 2}}).encode()
            else:
                payload = json.dumps({'error': {'code': 'server_error', 'message': 'fixture failure'}}).encode()
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            try:
                self.wfile.write(payload)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def log_message(self, *args):
            pass

    srv = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    worker = threading.Thread(target=srv.serve_forever, daemon=True)
    worker.start()
    monkeypatch.setenv('TEST_M_PROMPT_URL', f'http://127.0.0.1:{srv.server_port}/v1')
    monkeypatch.setenv('TEST_M_PROMPT_KEY', 'test')
    yield state
    srv.shutdown()
    srv.server_close()
    worker.join()


def make_prompt_actor(options=None, *, max_requests=None):
    pack = parse_prompt_pack(PACK)
    operation = OperatorLLMMapOp(prompt_name='enrich', config=pack,
                                 inputs={'payload': 'item'}, output='answer')
    coordinator = InProcessOperatorLLMCoordinator(max_requests=max_requests)
    actor = PromptActor(operation, pack, coordinator, options)
    return actor, operation, pack


# --- M001 build_model_factory -----------------------------------------------
def test_m001():
    services, store, port = make_services()
    bounds = services.bounds
    program_ref = store.put(iter([b'code']), max_bytes=1024, media_type='application/octet-stream')
    pack = parse_prompt_pack(PACK)
    prompt_op = OperatorLLMMapOp(prompt_name='enrich', config=pack,
                                 inputs={'payload': 'item'}, output='answer')
    # driver model construction count must stay zero while freezing config
    constructed = []
    import demiflow.operator_llm.client as client_mod
    import demiflow.embeddings.runtime as emb_mod
    original_client = client_mod.AsyncOperatorLLMClient.__init__
    original_emb = emb_mod.EmbeddingActor.__init__
    def counting_client(self, *a, **k):
        constructed.append('client'); raise AssertionError('factory constructed a model client')
    def counting_emb(self, *a, **k):
        constructed.append('embedding'); raise AssertionError('factory constructed an embedding actor')
    client_mod.AsyncOperatorLLMClient.__init__ = counting_client
    emb_mod.EmbeddingActor.__init__ = counting_emb
    try:
        spec = models.build_model_factory(prompt_op, program_ref=program_ref, bounds=bounds)
    finally:
        client_mod.AsyncOperatorLLMClient.__init__ = original_client
        emb_mod.EmbeddingActor.__init__ = original_emb
    assert constructed == []
    assert isinstance(spec, KernelSpec) and spec.kind == 'prompt'
    assert spec.implementation_ref is program_ref
    assert spec.effect == 'journaled' and spec.state == 'none'
    # configuration is strict declarative JSON (cloudpickle carries only code/config)
    json.dumps(_thaw(spec.configuration))
    assert spec.configuration['model']['name'] == 'mock-model'
    assert spec.configuration['content_hash'] == pack.content_hash

    emb_op = EmbeddingMapOp(callable=CallableSpec.create(lambda rows: rows),
                            inputs={'text': 'text'}, model_contract={'name': 'm'})
    assert models.build_model_factory(emb_op, program_ref=None, bounds=bounds).kind == 'embedding'
    agent_op = AgentMapOp(callable=CallableSpec.create(lambda row: row),
                          environment=OperatorEnvironment(runtime='codex', operators=()))
    agent_spec = models.build_model_factory(agent_op, program_ref=None, bounds=bounds)
    assert agent_spec.kind == 'agent' and agent_spec.effect == 'unknown_replay_forbidden'
    assert agent_spec.state == 'nonrecoverable'
    search_op = SearchWebOp(callable=CallableSpec.create(lambda row: row), output='hits')
    assert models.build_model_factory(search_op, program_ref=None, bounds=bounds).kind == 'fetch'
    doc_op = ReadDocumentsOp(callable=CallableSpec.create(lambda row: row), request='docs', output='out')
    assert models.build_model_factory(doc_op, program_ref=None, bounds=bounds).kind == 'document'

    with pytest.raises(DataflowError) as unsupported:
        models.build_model_factory(MapOp(callable=CallableSpec.create(lambda row: row)),
                                   program_ref=None, bounds=bounds)
    assert unsupported.value.code == 'UNSUPPORTED_CAPABILITY'
    live_op = OperatorLLMMapOp(prompt_name='enrich', config=pack, inputs={'payload': 'item'},
                               output='answer', options={'live': object()})
    with pytest.raises(DataflowError) as invalid:
        models.build_model_factory(live_op, program_ref=None, bounds=bounds)
    assert invalid.value.code == 'INVALID_CONTRACT'


# --- M002 bind_model_services -----------------------------------------------
def test_m002():
    services, store, port = make_services()
    model = EmbeddingModel('fixture', 'rev-1', 3, 'http://127.0.0.1:1/v1')
    actor = EmbeddingActor(model=model, inputs={'text': 'text'}, output='embedding',
                           call_output=None, error_output=None, concurrency=1, label='emb',
                           options={}, max_requests=None, service=None, request_gate=None)
    spec = KernelSpec(kind='embedding', implementation_ref=None,
                      configuration={'node_id': 'node-emb'}, inputs=(), state='none',
                      effect='journaled', resource_role='remote_api',
                      profile=ResourceProfile(requests={'cpu_millis': 100, 'memory_bytes': 1024}),
                      output_bound_bytes=1024, mergeable=False)
    bound = models.bind_model_services(actor, spec, services=services)
    assert bound is actor
    recovery = actor._native_recovery
    assert isinstance(recovery, RemoteEmbeddingRecovery)
    assert isinstance(recovery.journal, RemotePromptJournal)  # atomic budget via reserve RPC
    # Embedding has no Prompt coordinator and never borrows bind_client
    assert not hasattr(recovery, 'bind_client')
    assert not hasattr(actor, 'coordinator')
    with pytest.raises(DataflowError) as unknown:
        models.bind_model_services(actor, KernelSpec(
            kind='mystery', implementation_ref=None, configuration={'node_id': 'n'}, inputs=(),
            state='none', effect='pure', resource_role='cpu',
            profile=ResourceProfile(requests={'cpu_millis': 1, 'memory_bytes': 1}),
            output_bound_bytes=1, mergeable=False), services=services)
    assert unknown.value.code == 'UNSUPPORTED_CAPABILITY'


# --- M003 make_effect_transport ---------------------------------------------
def test_m003():
    services, store, port = make_services()
    transport = models.make_effect_transport(port, actor_id='node-1', bounds=services.bounds)
    digest = identity(['request', 'a'])
    eid = 'effect-1'
    assert transport.lookup(effect_command('lookup', eid, digest)).status == 'absent'
    assert transport.submit(effect_command('reserve', eid, digest)).status == 'reserved'
    assert transport.submit(effect_command('mark_sent', eid, digest)).status == 'sent'
    result_ref = store.put(iter([b'result']), max_bytes=1024, media_type='application/json')
    confirmed = transport.submit(effect_command('confirm', eid, digest, payload_ref=result_ref))
    assert confirmed.status == 'confirmed' and confirmed.result_ref is result_ref
    # confirmed-but-lost response is reused by effect_id, never re-sent
    reused = transport.lookup(effect_command('lookup', eid, digest))
    assert reused.status == 'confirmed' and reused.result_ref is result_ref
    # unknown effect refuses replay
    transport.submit(effect_command('unknown', eid, digest))
    with pytest.raises(DataflowError) as unknown:
        transport.submit(effect_command('reserve', eid, digest))
    assert unknown.value.code == 'UNKNOWN_EFFECT' and unknown.value.retryable is False
    # same effect_id with a different request_digest is rejected
    with pytest.raises(DataflowError) as mismatch:
        transport.lookup(effect_command('lookup', eid, identity(['request', 'b'])))
    assert mismatch.value.code == 'INVALID_CONTRACT'
    # actor mismatch and missing digest are rejected before any RPC
    bad = EffectCommand(operation_id=identity(['x']), kind='lookup', effect_id=eid,
                        actor_id='other', request_digest=digest, payload_ref=None, budget={})
    with pytest.raises(DataflowError):
        transport.lookup(bad)


# --- M004 restore_actor_state -----------------------------------------------
class StatefulActor:
    def __init__(self):
        self.total = 0
        self._lock = threading.Lock()
        self._task = None

    def snapshot_state(self):
        return {'total': self.total}

    def restore_state(self, state):
        self.total = state['total']


def test_m004():
    services, store, port = make_services()
    actor = StatefulActor()
    actor.total = 7
    actor._task = 'astart-task-handle'
    ref = models.snapshot_actor_state(actor, services=services)
    fresh = StatefulActor()
    fresh._lock = threading.Lock()
    models.restore_actor_state(fresh, ref=ref, services=services)
    assert fresh.total == 7  # business accumulator restored
    assert hasattr(fresh._lock, 'acquire') and hasattr(fresh._lock, 'release')  # resource rebuilt, not erased
    # None ref is a no-op
    before = fresh.total
    models.restore_actor_state(fresh, ref=None, services=services)
    assert fresh.total == before
    # half a hook pair is rejected
    class HalfHook:
        def snapshot_state(self):
            return {}
    with pytest.raises(DataflowError) as half:
        models.restore_actor_state(HalfHook(), ref=ref, services=services)
    assert half.value.code == 'INVALID_CONTRACT'
    # auto_json fallback restores public business attrs; empty vars are not stateful
    class Plain:
        def __init__(self):
            self.count = 3
            self._private = 'resource'
    plain = Plain()
    pref = models.snapshot_actor_state(plain, services=services)
    restored = Plain()
    restored.count = 0
    models.restore_actor_state(restored, ref=pref, services=services)
    assert restored.count == 3 and restored._private == 'resource'
    class Empty:
        pass
    eref = models.snapshot_actor_state(Empty(), services=services)
    assert json.loads(b''.join(store.read(eref, max_bytes=1024))) == {}


# --- M005 snapshot_actor_state ----------------------------------------------
def test_m005():
    services, store, port = make_services()
    actor = StatefulActor()
    actor.total = 42
    ref = models.snapshot_actor_state(actor, services=services)
    assert isinstance(ref, ArtifactRef)
    assert json.loads(b''.join(store.read(ref, max_bytes=services.bounds.max_state_bytes))) == {'total': 42}
    # opaque public state cannot be honestly reported as stateless then upgraded
    class Opaque:
        def __init__(self):
            self.handle = object()
    with pytest.raises(DataflowError) as opaque:
        models.snapshot_actor_state(Opaque(), services=services)
    assert opaque.value.code == 'INVALID_CONTRACT'
    # size is bounded up front
    small = make_services()
    small_services = small[0]
    object.__setattr__(small_services.bounds, 'max_state_bytes', 8) if False else None
    tight = RuntimeServices(store=small[1], journal=small[2],
                            bounds=make_bounds(max_state_bytes=8),
                            assignment=small_services.assignment, clock_ms=lambda: 0,
                            check_stop=lambda: None, constants={}, programs={})
    big = StatefulActor()
    big.total = 10**9
    with pytest.raises(DataflowError) as budget:
        models.snapshot_actor_state(big, services=tight)
    assert budget.value.code == 'BUDGET_EXCEEDED'


# --- M006 bind_cached_map ----------------------------------------------------
def test_m006(tmp_path):
    calls = []

    def compute(n):
        calls.append(n)
        return None if n == 0 else [n, n * 2]

    services, store, port = make_services(programs={
        'node-cache': LoadedProgram(kind='callable', value=compute, digest=identity(['compute']))})
    spec = KernelSpec(kind='transform', implementation_ref=None,
                      configuration={'node_id': 'node-cache',
                                     'cache': {'cache_dir': str(tmp_path / 'cache'), 'version': 'v1',
                                               'inputs': {'n': 'n'}, 'output': 'answer'}},
                      inputs=(), state='none', effect='pure', resource_role='cpu',
                      profile=ResourceProfile(requests={'cpu_millis': 1, 'memory_bytes': 1}),
                      output_bound_bytes=1024, mergeable=False)
    kernel = models.bind_cached_map(spec, services=services)
    kernel.start()
    record = Record(record_id=identity(['record', 0]), source_position={}, payload_ref=None,
                    inline={'n': 1}, encoded_bytes=8, sequence=0)
    out = list(kernel.push('in', record))
    assert thaw_record_value(out[0].inline) == {'n': 1, 'answer': [1, 2]}  # list result fidelity
    # second read reuses the shared result without recomputation
    out2 = list(kernel.push('in', Record(record_id=identity(['record', 1]), source_position={}, payload_ref=None,
                                         inline={'n': 1}, encoded_bytes=8, sequence=1)))
    assert thaw_record_value(out2[0].inline) == {'n': 1, 'answer': [1, 2]}
    assert calls == [1]
    none_out = list(kernel.push('in', Record(record_id=identity(['record', 2]), source_position={}, payload_ref=None,
                                             inline={'n': 0}, encoded_bytes=8, sequence=2)))
    assert none_out[0].inline == {'n': 0, 'answer': None}  # None result fidelity
    # cache Path/lock are coordination resources, not business state
    assert kernel.snapshot() is None
    assert kernel.restore(None) is None
    kernel.close()

    # bytes payload fidelity: inline preserves the exact object, no JSON corruption
    def bytes_compute(n):
        return b'\x00\x01binary'
    from demiflow.data.result_cache import CachedSyncMap
    cached = CachedSyncMap(bytes_compute, str(tmp_path / 'bytes2'), 'v1',
                           inputs={'n': 'n'}, output='answer', cache_when=lambda value: False)
    result = cached({'n': 1})
    assert result['answer'] == b'\x00\x01binary'

    with pytest.raises(DataflowError) as missing:
        models.bind_cached_map(KernelSpec(kind='transform', implementation_ref=None,
                                          configuration={'node_id': 'x'}, inputs=(), state='none',
                                          effect='pure', resource_role='cpu',
                                          profile=ResourceProfile(requests={'cpu_millis': 1, 'memory_bytes': 1}),
                                          output_bound_bytes=1, mergeable=False), services=services)
    assert missing.value.code == 'INVALID_CONTRACT'


# --- M007 embedding_cache_key -----------------------------------------------
def test_m007():
    base = dict(model_digest='model-a', payload_digest='payload-a',
                batch_semantics={'task': 'embed', 'normalize': True, 'concurrency': 1, 'max_batch': 8})
    key = models.embedding_cache_key('rec-1', **base)
    # pure packing/concurrency changes keep the per-record result identity
    for knob in ('concurrency', 'max_batch', 'batch_size', 'queue_depth', 'flush_interval', 'packing'):
        changed = {**base, 'batch_semantics': {**base['batch_semantics'], knob: 99}}
        assert models.embedding_cache_key('rec-1', **changed) == key
    # model / normalization / task semantics never reuse a result
    assert models.embedding_cache_key('rec-1', **{**base, 'model_digest': 'model-b'}) != key
    assert models.embedding_cache_key('rec-1', **{**base, 'payload_digest': 'payload-b'}) != key
    assert models.embedding_cache_key('rec-2', **base) != key
    norm = {**base, 'batch_semantics': {**base['batch_semantics'], 'normalize': False}}
    assert models.embedding_cache_key('rec-1', **norm) != key
    task = {**base, 'batch_semantics': {**base['batch_semantics'], 'task': 'other'}}
    assert models.embedding_cache_key('rec-1', **task) != key
    # distinct from the single-request idempotency key
    assert key != request_key(req())


# --- M008 RemotePromptJournal.__init__ --------------------------------------
def test_m008():
    services, store, port = make_services(limits={'max_requests': 50, 'max_control_bytes': 16 * 1024**2})
    remote, action = make_journal(services)
    assert remote.limit == 50 and remote.read_only is False
    assert remote.max_record_bytes == min(16 * 1024**2, services.bounds.max_control_bytes)
    assert remote.timeout_s == services.bounds.timeout_ms / 1000.0
    # no SQLite path / database is owned by the worker journal
    assert not hasattr(remote, 'path') and not hasattr(remote, '_connect')
    # undeclared methods are not forwarded through __getattr__
    for name in ('begin_attempt', 'commit', 'publication_guard', 'bind_execution_action', 'reserve_budgets'):
        with pytest.raises(AttributeError):
            getattr(remote, name)
    # closing the worker journal does not close the driver journal
    remote.close()
    assert port.closed is False
    other, _ = make_journal(services, node_id='node-2')
    with native_request_budget(other._action.context, node_id='node-2', max_requests=None):
        assert other.reserve(req()) is True


# --- M009 RemotePromptJournal.lookup ----------------------------------------
def test_m009():
    services, store, port = make_services()
    remote, action = make_journal(services)
    request = req()
    assert remote.lookup(request) is None  # absent
    with native_request_budget(action.context, node_id='node-1', max_requests=None):
        assert remote.reserve(request) is True
    # pending (reserved, no response) must NOT be treated as a cache miss
    with pytest.raises(DataflowError) as pending:
        remote.lookup(request)
    assert pending.value.code == 'UNKNOWN_EFFECT'
    record = {'status_code': 200, 'body': {'choices': [{'message': {'content': 'x'}}]}, 'elapsed_s': 0.1}
    remote.response(request, record)
    assert remote.lookup(request) == record  # confirmed result reused
    saved, refs = remote.lookup_with_references(request)
    assert saved == record and refs['response_ref']['kind'] == 'response'
    # the original request_key travels to the driver unchanged
    assert port.commands[0].request_key == request.request_key


# --- M010 RemotePromptJournal.reserve ---------------------------------------
def test_m010():
    services, store, port = make_services()
    remote, action = make_journal(services)
    request = req()
    # missing CALL_RESERVATION is rejected (Embedding missing charges refused by driver)
    with pytest.raises(DataflowError) as missing:
        remote.reserve(request)
    assert missing.value.code == 'INVALID_CONTRACT'
    with native_request_budget(action.context, node_id='node-1', max_requests=None) as reservation:
        assert remote.reserve(request) is True
    command = port.commands[-1]
    assert command.operation == 'reserve'
    assert command.reservation[0] == reservation
    assert command.reservation[1]['requests'] == (1, 100)
    # budget and intent travel in ONE RPC; no separate generic effect.reserve
    assert port.effect_commands == []
    assert port.reserve_calls == []
    # an explicit 0 ceiling forbids fresh dispatch and is not collapsed to None
    zero_action = journal.make_native_action_view(services, node_id='node-zero')
    with native_request_budget(zero_action.context, node_id='node-zero', max_requests=0):
        with pytest.raises(DataflowError) as zero:
            remote.reserve(req({'x': 2}))
        assert zero.value.code == 'BUDGET_EXCEEDED'


# --- M011 RemotePromptJournal.reserve_http_retry ----------------------------
def test_m011():
    services, store, port = make_services()
    remote, action = make_journal(services)
    request = req()
    with pytest.raises(DataflowError) as unknown_kwargs:
        remote.reserve_http_retry(request, expected_attempt=1, expected_status=500,
                                  max_attempts=3, requeue_uncertain=True)
    assert unknown_kwargs.value.code == 'INVALID_CONTRACT'
    with pytest.raises(DataflowError):
        remote.reserve_http_retry(request, expected_attempt=0, expected_status=500, max_attempts=3)
    with pytest.raises(DataflowError):
        remote.reserve_http_retry(request, expected_attempt=1, expected_status=200, max_attempts=3)
    with pytest.raises(DataflowError):
        remote.reserve_http_retry(request, expected_attempt=1, expected_status=500, max_attempts=9)
    # a complete saved HTTP error response can be retried within budget
    with native_request_budget(action.context, node_id='node-1', max_requests=None):
        remote.reserve(request)
    remote.response(request, {'status_code': 500, 'body': {'error': {'code': 'server_error'}},
                              'elapsed_s': 0.2})
    with native_request_budget(action.context, node_id='node-1', max_requests=None):
        assert remote.reserve_http_retry(request, expected_attempt=1, expected_status=500,
                                         max_attempts=3) is True
    assert port.commands[-1].operation == 'reserve_http_retry'
    # an uncertain (timeout) call is never retryable
    uncertain = req({'x': 5})
    with native_request_budget(action.context, node_id='node-1', max_requests=None):
        remote.reserve(uncertain)
    timeout = TimeoutError('read timed out')
    timeout.call = {'http_status': None}
    remote.failed(uncertain, timeout, 0.3)
    with native_request_budget(action.context, node_id='node-1', max_requests=None):
        with pytest.raises(DataflowError) as not_retryable:
            remote.reserve_http_retry(uncertain, expected_attempt=1, expected_status=500, max_attempts=3)
    assert not_retryable.value.code == 'UNKNOWN_EFFECT'


# --- M012 RemotePromptJournal.response --------------------------------------
def test_m012():
    services, store, port = make_services()
    remote, action = make_journal(services)
    request = req()
    with native_request_budget(action.context, node_id='node-1', max_requests=None):
        remote.reserve(request)
    record = {'status_code': 200, 'body': {'choices': [{'message': {'content': 'saved'}}]},
              'elapsed_s': 0.1, 'image': {'uri': 'file:///img', 'sha256': identity(['img'])}}
    refs = remote.response(request, record)
    assert refs['response_ref']['kind'] == 'response'
    # the bounded result was staged to an artifact before the response commit
    response_cmd = [c for c in port.commands if c.operation == 'response'][-1]
    assert response_cmd.value_ref is not None
    assert json.loads(b''.join(store.read(response_cmd.value_ref, max_bytes=16 * 1024**2))) == record
    # after saving, a disconnect reuses the same key without a second network request
    assert remote.lookup(request) == record
    assert port.requests[request.request_key]['status'] == 'confirmed'


# --- M013 RemotePromptJournal.failed ----------------------------------------
def test_m013():
    services, store, port = make_services()
    remote, action = make_journal(services)
    request = req()
    with native_request_budget(action.context, node_id='node-1', max_requests=None):
        remote.reserve(request)
    for bad_elapsed in (-1.0, float('inf'), float('nan'), '0.1'):
        with pytest.raises(DataflowError) as invalid:
            remote.failed(request, RuntimeError('x'), bad_elapsed)
        assert invalid.value.code == 'INVALID_CONTRACT'
    # a timeout is NOT automatically definite-unsent
    timeout = TimeoutError('read timed out')
    timeout.call = {'http_status': None, 'timeout_phase': 'read_idle'}
    remote.failed(request, timeout, 0.5)
    assert port.requests[request.request_key]['status'] == 'unknown'
    # only the existing error contract travels (type/detail/elapsed/call), no pickle
    failed_cmd = [c for c in port.commands if c.operation == 'failed'][-1]
    payload = json.loads(b''.join(store.read(failed_cmd.value_ref, max_bytes=16 * 1024**2)))
    assert payload['type'] == 'TimeoutError' and payload['elapsed_s'] == 0.5
    assert payload['call']['timeout_phase'] == 'read_idle'
    # definite-unsent evidence is preserved
    unsent = RuntimeError('cancelled before dispatch')
    unsent.call = {'request_sent': False, 'batch_cancelled_before_dispatch': True}
    remote.failed(req({'x': 9}), unsent, 0.1)
    assert port.requests[request_key(req({'x': 9}))]['status'] == 'unsent'


# --- M014 RemotePromptJournal.references ------------------------------------
def test_m014():
    services, store, port = make_services()
    remote, action = make_journal(services)
    request = req()
    with native_request_budget(action.context, node_id='node-1', max_requests=None):
        remote.reserve(request)
    remote.response(request, {'status_code': 200, 'body': {}, 'elapsed_s': 0.1})
    refs = remote.references(request)
    assert refs['request_id'] == request.request_key
    assert refs['response_ref']['kind'] == 'response'
    # references are opaque driver tokens, not worker-readable local absolute paths
    assert refs['journal_path'] == 'driver-native-journal'
    assert not Path(refs['journal_path']).is_absolute()
    assert not hasattr(remote, 'read_reference') and not hasattr(remote, 'paths')


# --- M015 RemotePromptJournal.media_references ------------------------------
def test_m015():
    services, store, port = make_services()
    remote, action = make_journal(services)
    request = req()
    with native_request_budget(action.context, node_id='node-1', max_requests=None):
        remote.reserve(request)
    remote.response(request, {'status_code': 200, 'body': {}, 'elapsed_s': 0.1})
    # a cached image that lost its owned object cannot return success
    with pytest.raises(DataflowError) as lost:
        remote.media_references(request, register=True)
    assert lost.value.code == 'CORRUPT_ARTIFACT'
    # register travels through the driver native media operation
    port.requests[request.request_key]['media'] = ('objects/img-1',)
    names = remote.media_references(request, register=True)
    assert names == ('objects/img-1',)
    assert port.commands[-1].operation == 'media_references'
    assert port.commands[-1].options['register'] is True
    with pytest.raises(DataflowError):
        remote.media_references(request, register='yes')


# --- M016 RemotePromptJournal.close -----------------------------------------
def test_m016():
    services, store, port = make_services()
    first, _ = make_journal(services, node_id='node-1')
    second, action2 = make_journal(services, node_id='node-2')
    with native_request_budget(action2.context, node_id='node-2', max_requests=None):
        second.reserve(req({'x': 1}))
    first.close()
    # one actor ending does not affect another node's confirming request
    record = {'status_code': 200, 'body': {}, 'elapsed_s': 0.1}
    second.response(req({'x': 1}), record)
    assert second.lookup(req({'x': 1})) == record
    assert port.closed is False
    with pytest.raises(DataflowError) as closed:
        first.lookup(req())
    assert closed.value.code == 'CANCELLED'


# --- M017 RemotePromptRecovery.__init__ / bind_client -----------------------
def test_m017(prompt_server):
    services, store, port = make_services()
    actor, operation, pack = make_prompt_actor()
    action = journal.make_native_action_view(services, node_id='node-1')
    recovery = RemotePromptRecovery(actor, action, 'node-1', services=services)
    assert isinstance(recovery.journal, RemotePromptJournal)
    assert recovery.row_local is True and recovery.copy_inputs is False
    actor._runtime.native_recovery = recovery
    actor._runtime.coordinator.bind_execution(action.context, 'node-1')
    actor._runtime.coordinator._native_group_journal = True
    set_row_scope(RowScope(occurrence='occ-1'))
    try:
        row = asyncio.run(actor({'item': {'n': 1}}))
    finally:
        set_row_scope(None)
    assert row['answer'] == 'ok'
    assert len(prompt_server['requests']) == 1
    ops = [c.operation for c in port.commands]
    assert 'reserve' in ops and 'response' in ops
    # the original record/prepare semantics produced the native request_key
    reserved = [c for c in port.commands if c.operation == 'reserve'][0]
    assert reserved.request_key == request_key(
        RequestRecord({**json.loads(b''.join(store.read(reserved.request_ref, max_bytes=16 * 1024**2)))}))
    asyncio.run(actor.aclose())


# --- M018 RemoteEmbeddingRecovery.__init__ ----------------------------------
def test_m018():
    services, store, port = make_services()
    model = EmbeddingModel('fixture', 'rev-1', 3, 'http://127.0.0.1:1/v1')
    actor = EmbeddingActor(model=model, inputs={'text': 'text'}, output='embedding',
                           call_output=None, error_output=None, concurrency=1, label='emb',
                           options={}, max_requests=None, service=None, request_gate=None)
    action = journal.make_native_action_view(services, node_id='node-emb')
    recovery = RemoteEmbeddingRecovery(actor, action, 'node-emb', services=services)
    assert isinstance(recovery.journal, RemotePromptJournal)
    assert recovery.row_local is True and recovery.inflight == {}
    assert recovery._response_bound is None
    # no Prompt coordinator is fabricated and bind_client is not borrowed
    assert not hasattr(recovery, 'bind_client') and not hasattr(recovery, '_coordinator')
    # a private SQLite journal is a second authority and is refused
    actor_private = EmbeddingActor(model=model, inputs={'text': 'text'}, output='embedding',
                                   call_output=None, error_output=None, concurrency=1, label='emb',
                                   options={'sqlite_journal': {'path': '/tmp/demiflow-m-private.sqlite'}},
                                   max_requests=None, service=None, request_gate=None)
    with pytest.raises(RecoveryContractError):
        RemoteEmbeddingRecovery(actor_private, action, 'node-emb', services=services)


# --- M019 RemoteEmbeddingRecovery.verify_request_membership -----------------
def test_m019():
    services, store, port = make_services()
    model = EmbeddingModel('fixture', 'rev-1', 3, 'http://127.0.0.1:1/v1')
    actor = EmbeddingActor(model=model, inputs={'text': 'text'}, output='embedding',
                           call_output=None, error_output=None, concurrency=1, label='emb',
                           options={}, max_requests=None, service=None, request_gate=None)
    action = journal.make_native_action_view(services, node_id='node-emb')
    recovery = RemoteEmbeddingRecovery(actor, action, 'node-emb', services=services)
    set_row_scope(RowScope(occurrence='occ-batch'))
    try:
        request = recovery.scope_request(req({'input': ['a', 'b']}))
    finally:
        set_row_scope(None)
    recovery.verify_request_membership(request)
    command = port.commands[-1]
    assert command.operation == 'bind_membership'
    assert command.options['request_key'] == request.request_key
    assert command.options['membership_key'] == identity(
        ['embedding-request-membership', request['native_occurrence']])
    # the same occurrence with different batch membership is rejected
    set_row_scope(RowScope(occurrence='occ-batch'))
    try:
        other = recovery.scope_request(req({'input': ['a', 'c']}))
    finally:
        set_row_scope(None)
    with pytest.raises(DataflowError) as changed:
        recovery.verify_request_membership(other)
    assert changed.value.code == 'INCOMPATIBLE_HISTORY'


# --- M020 bind_prompt_adapter -----------------------------------------------
def test_m020():
    services, store, port = make_services()
    actor, operation, pack = make_prompt_actor()
    actor._runtime.concurrency = 4
    actor._request_policy = tuple(sorted({'max_concurrency': 2, 'initial_concurrency': 2,
                                          'min_concurrency': 1}.items()))
    journal.bind_prompt_adapter(actor, services=services, node_id='node-1')
    recovery = actor._runtime.native_recovery
    assert isinstance(recovery, RemotePromptRecovery)
    coordinator = actor._runtime.coordinator
    assert coordinator._native_group_journal is True
    # request_gate rebuilt from the declarative policy
    assert isinstance(actor._runtime.request_gate, AdaptiveRequestGate)
    # CALL_RESERVATION survives a real BlockingIOPool thread hop
    reservation = coordinator.reserve()
    seen = {}

    def read_reservation():
        seen['value'] = CALL_RESERVATION.get()
        return True

    async def drive():
        pool = BlockingIOPool(1, name='demiflow-m-test')
        try:
            await pool.run(read_reservation)
        finally:
            await pool.aclose()

    asyncio.run(drive())
    assert seen['value'][0] == reservation
    assert seen['value'][1]['requests'] == (1, 100)
    coordinator.failed(reservation)
    CALL_RESERVATION.set(None)


# --- M021 bind_embedding_adapter --------------------------------------------
def test_m021():
    services, store, port = make_services()
    state = {'bodies': []}
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            with lock:
                state['bodies'].append(body)
            values = body.get('input', [])
            response = dict(model='fixture', data=[dict(index=i, embedding=[3., 4., float(i)])
                                                   for i in range(len(values))],
                            usage={'prompt_tokens': 7})
            payload = json.dumps(response).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *args):
            pass

    srv = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    worker = threading.Thread(target=srv.serve_forever, daemon=True)
    worker.start()
    try:
        model = EmbeddingModel('fixture', 'rev-1', 3, f'http://127.0.0.1:{srv.server_port}/v1')
        actor = EmbeddingActor(model=model, inputs={'text': 'text'}, output='embedding',
                               call_output=None, error_output=None, concurrency=1, label='emb',
                               options={}, max_requests=None, service=None, request_gate=None)
        journal.bind_embedding_adapter(actor, services=services, node_id='node-emb')
        assert isinstance(actor._native_recovery, RemoteEmbeddingRecovery)
        assert not hasattr(actor, 'coordinator')  # no Prompt coordinator needed

        async def drive():
            await actor.astart()
            assert isinstance(actor._journal, _BorrowedJournal)
            set_row_scope(RowScope(occurrence='occ-emb'))
            try:
                rows = await actor([{'text': 'a'}, {'text': 'b'}])
            finally:
                set_row_scope(None)
            await actor.aclose()
            return rows

        rows = asyncio.run(drive())
        assert len(rows) == 2 and all('embedding' in r for r in rows)
        ops = [c.operation for c in port.commands]
        assert 'bind_membership' in ops and 'reserve' in ops and 'response' in ops
        # run budget and node budget are charged in the same reserve RPC
        reserve_cmd = [c for c in port.commands if c.operation == 'reserve'][0]
        charges = reserve_cmd.reservation[1]
        assert 'requests' in charges and any(k.startswith('requests-') for k in charges)
        assert len(state['bodies']) == 1
    finally:
        srv.shutdown()
        srv.server_close()
        worker.join()


# --- M022 bind_agent_adapter ------------------------------------------------
def test_m022():
    services, store, port = make_services()
    # demiflow runtime: model subcalls reuse the native journal and run quota
    actor, operation, pack = make_prompt_actor()
    actor.environment = OperatorEnvironment(runtime='demiflow', operators=())
    journal.bind_agent_adapter(actor, services=services, node_id='node-agent')
    assert isinstance(actor._runtime.native_recovery, RemotePromptRecovery)
    assert actor._runtime.native_recovery.row_local is False
    assert actor._runtime.coordinator._native_group_journal is True
    # codex runtime keeps its real recovery level: unknown requests never auto-replay
    codex_actor, _, _ = make_prompt_actor()
    codex_actor.environment = OperatorEnvironment(runtime='codex', operators=())
    journal.bind_agent_adapter(codex_actor, services=services, node_id='node-codex')
    from demiflow.execution.dataflow_model_journal import RemoteCodexRecovery
    assert isinstance(codex_actor._runtime.native_recovery, RemoteCodexRecovery)
    assert codex_actor._runtime.native_recovery.row_local is False
    coordinator = codex_actor._runtime.coordinator
    assert coordinator._native_group_journal is True
    token = CALL_RESERVATION.set(None)
    try:
        reservation = coordinator.reserve()
        staged, charges = CALL_RESERVATION.get()
        assert staged == reservation
        assert charges['requests'] == (1, services.journal.limits['max_requests'])
        assert any(k.startswith('requests-') for k in charges)
        assert not port.reserve_calls  # intent and both ceilings commit atomically through reserve
        coordinator.failed(reservation)
    finally:
        CALL_RESERVATION.reset(token)


# --- M023 bind_media_adapter ------------------------------------------------
def test_m023():
    services, store, port = make_services()
    actor, operation, pack = make_prompt_actor(options={'object_store': 'images'})
    journal.bind_media_adapter(actor, services=services, node_id='node-media')
    assert isinstance(actor._runtime.native_recovery, RemotePromptRecovery)
    from demiflow.execution.dataflow_images import RemoteImageStore
    assert isinstance(actor._runtime.artifact_store, RemoteImageStore)
    assert actor._runtime.artifact_store.store is services.store
    assert actor._runtime.artifact_store.journal is actor._runtime.native_recovery.journal
    assert actor._runtime.artifact_store.maximum > 0
    assert actor._runtime.coordinator._native_group_journal is True
    assert actor._runtime.coordinator._artifact_request_budget == 'image-requests-' + identity('action-1')
    # missing object participant refuses to start
    no_store, _, _ = make_services()
    plain_actor, _, _ = make_prompt_actor()
    with pytest.raises(DataflowError) as missing:
        journal.bind_media_adapter(plain_actor, services=no_store, node_id='node-media')
    assert missing.value.code == 'INVALID_CONTRACT'


# --- M024 make_native_action_view -------------------------------------------
def test_m024():
    services, store, port = make_services(limits={'max_requests': 7, 'max_control_bytes': 16 * 1024**2})
    view = journal.make_native_action_view(services, node_id='node-1')
    assert isinstance(view, NativeActionView)
    assert view.action_id == 'action-1'
    assert view.context.limits['max_requests'] == 7
    assert view.context.fence == 'fence-1'
    assert view.native_image_effects is True
    # the read-only view exposes exactly limits/fence/check_stop, no full RunContext
    assert {f.name for f in fields(view.context)} == {'limits', 'fence', 'check_stop'}
    for name in ('directory', 'path', 'publication_guard', 'journal'):
        assert not hasattr(view.context, name)
    # unknown capabilities are rejected by the port, never forwarded
    with pytest.raises(DataflowError) as unknown:
        port.queue(object())
    assert unknown.value.code == 'UNSUPPORTED_CAPABILITY'


# --- M025 AsyncOperatorLLMClient native_journal injection -------------------
def test_m025(prompt_server):
    services, store, port = make_services()
    remote, action = make_journal(services)
    pack = parse_prompt_pack(PACK)
    model = pack.prompt_definitions['enrich'].model
    options = {'http_error_retry': {'statuses': [500], 'max_retries': 2, 'backoff_s': [0.0, 0.0]}}
    client = AsyncOperatorLLMClient(model, options, native_journal=remote)
    assert client.journal is remote
    # a non-capability-verified journal is rejected (not merely hasattr)
    with pytest.raises(ValueError):
        AsyncOperatorLLMClient(model, options, native_journal=object())
    # a private write journal cannot dual-write with native recovery
    with pytest.raises(ValueError):
        AsyncOperatorLLMClient(model, {'sqlite_journal': {'path': '/tmp/demiflow-m-dual.sqlite'}},
                               native_journal=remote)
    # read-only / invalid limit remote journals are rejected
    remote.read_only = True
    with pytest.raises(ValueError):
        AsyncOperatorLLMClient(model, options, native_journal=remote)
    remote.read_only = False
    # end-to-end: real Prompt + http_error_retry over the native journal (Local)
    actor, operation, _ = make_prompt_actor(options)
    recovery = RemotePromptRecovery(actor, action, 'node-1', services=services)
    actor._runtime.native_recovery = recovery
    actor._runtime.coordinator.bind_execution(action.context, 'node-1')
    actor._runtime.coordinator._native_group_journal = True
    prompt_server['fail_first'] = 1
    set_row_scope(RowScope(occurrence='occ-retry'))
    try:
        row = asyncio.run(actor({'item': {'n': 1}}))
    finally:
        set_row_scope(None)
    assert row['answer'] == 'ok'
    assert len(prompt_server['requests']) == 2  # one 500, one retried 200
    ops = [c.operation for c in port.commands]
    assert 'reserve_http_retry' in ops and 'response' in ops
    # an unknown effect is never made retryable by the injection
    assert ERRORS.DataflowError('UNKNOWN_EFFECT', 'x').retryable is False
    with pytest.raises(ValueError):
        ERRORS.DataflowError('UNKNOWN_EFFECT', 'x', retryable=True)
    asyncio.run(actor.aclose())


# --- M026 create_async_operator_llm_client factory --------------------------
def test_m026(prompt_server, tmp_path):
    services, store, port = make_services()
    remote, action = make_journal(services)
    pack = parse_prompt_pack(PACK)
    model = pack.prompt_definitions['enrich'].model
    client = create_async_operator_llm_client(model, None, native_journal=remote)
    assert isinstance(client, AsyncOperatorLLMClient) and client.journal is remote
    # Offline data now uses the same verified native port, never a worker-owned
    # database or a paid HTTP request.
    from demiflow.execution.dataflow_offline import RemoteOfflinePromptClient
    before = list(port.commands)
    for options in ({'offline_store': {'path': str(tmp_path / 'offline.sqlite')}},
                    {'offline_dir': str(tmp_path / 'offline')}):
        offline = create_async_operator_llm_client(model, options, native_journal=remote)
        assert isinstance(offline, RemoteOfflinePromptClient) and offline.journal is remote
        assert offline.options == options and not list(tmp_path.iterdir())
        asyncio.run(offline.aclose())
        with pytest.raises(ValueError, match='verified native worker port'):
            create_async_operator_llm_client(model, options, native_journal=object())
    assert port.commands == before and prompt_server['requests'] == []
    # Codex exec binds the same verified port (test_runtime_v3_codex_prompt_journal.py).
    for options in ({'offline_dir': 'x', 'offline_store': {}},
                    {'offline_store': {}, 'sqlite_journal': {'path': 'not-owned'}},
                    {'offline_dir': 'x', 'http_error_retry': {'max_retries': 1}}):
        with pytest.raises(ValueError):
            create_async_operator_llm_client(model, options, native_journal=remote)
    # without injection the original construction path is unchanged
    plain = create_async_operator_llm_client(model)
    assert plain.journal is None
    asyncio.run(plain.aclose())
    asyncio.run(client.aclose())


# --- M027 AsyncOperatorLLMRuntime.call_with_trace ---------------------------
def test_m027(prompt_server):
    services, store, port = make_services()
    actor, operation, pack = make_prompt_actor(
        {'http_error_retry': {'statuses': [500], 'max_retries': 2, 'backoff_s': [0.0, 0.0]}})
    action = journal.make_native_action_view(services, node_id='node-1')
    recovery = RemotePromptRecovery(actor, action, 'node-1', services=services)
    actor._runtime.native_recovery = recovery
    actor._runtime.coordinator.bind_execution(action.context, 'node-1')
    actor._runtime.coordinator._native_group_journal = True
    prompt_server['fail_first'] = 1
    set_row_scope(RowScope(occurrence='occ-trace'))
    try:
        result, trace = asyncio.run(actor._runtime.call_with_trace('enrich', {'payload': {'n': 1}}))
    finally:
        set_row_scope(None)
    assert result == {'result': 'ok'}
    # the recovery journal was passed to the factory before bind_client
    client = actor._runtime._clients[pack.prompt_definitions['enrich'].model]
    assert isinstance(client.journal, _BorrowedJournal)
    assert client.journal.journal is recovery.journal
    assert len(prompt_server['requests']) == 2
    ops = [c.operation for c in port.commands]
    assert 'reserve' in ops and 'reserve_http_retry' in ops and 'response' in ops
    # request_policy is not bypassed: a declared gate still governs admission
    assert trace['attempts']
    asyncio.run(actor.aclose())