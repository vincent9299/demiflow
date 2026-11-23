"""V033: real sealed artifacts, worker loaders, grants and public execution.

Fault injection only changes owned test artifacts, and restores their exact
bytes before the public pipeline continues. Observers delegate to the original
compiler, allocator, store, worker and commit authorities. Component attacks
are explicitly distinguished from the hosted Dataset positive control.
"""
from __future__ import annotations

from dataclasses import asdict, replace
import hashlib
import io
import json
from pathlib import Path
import time
from unittest.mock import patch
import zipfile


REQUIRED_CASES = frozenset({
    'wrong_code_pin', 'worker_code_pin', 'cross_domain', 'worker_cross_domain', 'path_traversal',
    'symlink', 'checksum', 'dishonest_size', 'expired_grant',
    'missing_historical_plan', 'missing_historical_program',
    'missing_historical_source', 'oversized_control', 'oversized_bootstrap',
})


def _summarize(row):
    return {'value': row['value'] + 1, 'payload_bytes': len(row['payload'])}


def _rejection(call, *, code, text, **facts):
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    try:
        call()
    except DataflowError as error:
        assert error.code == code, (code, str(error))
        assert text in str(error), str(error)
        return {'status': 'rejected', 'exception': type(error).__name__,
                'code': error.code, 'message': str(error), **facts}
    raise AssertionError('malformed artifact was accepted: ' + text)


def _read(store, ref, bounds):
    return b''.join(store.read(ref, max_bytes=bounds.max_state_bytes))


def _put(store, raw, media='application/zip'):
    return store.put(iter((raw,)), max_bytes=max(1, len(raw)), media_type=media)


def _zip(name, payload, *, symlink=False):
    raw = io.BytesIO()
    with zipfile.ZipFile(raw, 'w') as archive:
        entry = zipfile.ZipInfo(name)
        entry.create_system = 3
        if symlink:
            entry.external_attr = 0o120777 << 16
        archive.writestr(entry, payload)
    return raw.getvalue()


def _mutate(store, ref, invoke, *, replacement=None):
    """A test-owned content fault; preserve the original ref and journal pins."""
    path = store._object_path(ref)
    original = path.read_bytes()
    assert hashlib.sha256(original).hexdigest() == ref.digest
    try:
        if replacement is None:
            path.unlink()
        else:
            path.write_bytes(replacement)
        result = invoke()
    finally:
        path.write_bytes(original)
        assert store.inspect(ref) and hashlib.sha256(path.read_bytes()).hexdigest() == ref.digest
    result['restored_sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()
    result['original_sha256'] = ref.digest
    return result


def _artifact_attacks(execution, root):
    from demiflow.execution.seal.runtime_bundle import fetch_runtime_bundle
    from demiflow.execution.seal.plan import (verify_dataflow_program, load_dataflow,
                                                _artifact_from_json)
    from demiflow.execution.seal.business_sources import mount_sources
    from demiflow.execution.recovery_contract import identity
    store, bounds, sealed = execution._store, execution._bounds, execution._sealed
    node = next(n for n in execution._graph.nodes if n.kind == 'transform')
    pin = json.loads(_read(store, sealed.code_pin_ref, bounds))
    manifest = json.loads(_read(store, node.program_ref, bounds))
    expected = pin['programs'][node.occurrence_id]
    result = {}
    good = verify_dataflow_program(node.program_ref, expected_pin=expected, store=store, bounds=bounds)
    assert good.digest == identity(expected)
    result['wrong_code_pin'] = _rejection(lambda: verify_dataflow_program(node.program_ref,
        expected_pin={'changed': True}, store=store, bounds=bounds),
        code='INVALID_CONTRACT', text='code pin differs', ref=asdict(node.program_ref))
    result['cross_domain'] = _rejection(lambda: _read(store,
        replace(node.program_ref, store_id='unrelated-domain'), bounds),
        code='INVALID_CONTRACT', text='another store domain')
    for name, member, symlink, message in (
            ('path_traversal', '../escaped.py', False, 'escapes its root'),
            ('symlink', 'linked.py', True, 'is a symlink')):
        ref = _put(store, _zip(member, b'never executed', symlink=symlink))
        cache = root / name
        result[name] = _rejection(lambda: fetch_runtime_bundle(ref, cache=cache,
            store=store, bounds=bounds), code='INVALID_CONTRACT', text=message, ref=asdict(ref))
        assert not list(cache.iterdir()), 'failed extraction published or leaked a bundle'
        assert not (root / 'escaped.py').exists()
    payload_ref = _artifact_from_json(manifest['payload_ref'])
    original_payload = _read(store, payload_ref, bounds)
    changed_payload = bytes((original_payload[0] ^ 1,)) + original_payload[1:]
    result['checksum'] = _mutate(store, payload_ref, lambda: _rejection(
        lambda: verify_dataflow_program(node.program_ref, expected_pin=expected, store=store, bounds=bounds),
        code='INVALID_CONTRACT', text='bytes do not match', ref=asdict(payload_ref)),
        replacement=changed_payload)
    ref = _put(store, b'original size bytes', 'application/octet-stream')
    result['dishonest_size'] = _rejection(lambda: _read(store,
        replace(ref, size_bytes=ref.size_bytes - 1), bounds),
        code='INVALID_CONTRACT', text='size differs')
    original_load = lambda: load_dataflow(execution._action.context.directory,
        execution._action.action_id, store=store, bounds=bounds)
    result['missing_historical_plan'] = _mutate(store, sealed.logical_ref, lambda: _rejection(
        original_load, code='UNKNOWN_EFFECT', text='definitely absent', ref=asdict(sealed.logical_ref)))
    result['missing_historical_program'] = _mutate(store, payload_ref, lambda: _rejection(
        lambda: verify_dataflow_program(node.program_ref, expected_pin=expected,
            store=store, bounds=bounds), code='UNKNOWN_EFFECT', text='definitely absent',
        ref=asdict(payload_ref), current_callable_available=callable(_summarize)))
    # The current callable's import is already available. Its existence cannot
    # substitute for the original archived source even with an empty cache.
    sources = manifest['business_sources']
    assert sources, 'the positive program must archive a real by-reference business module'
    source_name = next(iter(sources))
    source_ref = _artifact_from_json(sources[source_name])
    result['missing_historical_source'] = _mutate(store, source_ref, lambda: _rejection(
        lambda: mount_sources(sources, expected['imports'], store=store, cache=root/'missing-source-cache'),
        code='UNKNOWN_EFFECT', text='definitely absent', ref=asdict(source_ref), module=source_name))
    assert original_load()[2] == sealed
    assert verify_dataflow_program(node.program_ref, expected_pin=expected,
                                   store=store, bounds=bounds).digest == good.digest
    return result


class _NativeTransport:
    """In-process transport only; the original driver dispatch makes decisions."""
    def __init__(self, services):
        self.store = services.store
        self.service = services.journal_service
        self.requests = []

    def request(self, event, *, deadline_ms):
        self.requests.append(event)
        return self.service.dispatch(event)

    def check_stop(self):
        self.service._action.context.check_stop()


def _assignment_attacks(execution, assignment, root):
    from demiflow.execution.worker.runtime import WorkerJournalClient
    from demiflow.execution.seal.plan import load_worker_programs as _load_programs
    from demiflow.execution.worker.runtime import make_runtime_services
    from demiflow.execution.contracts.dataflow import WorkerEvent
    from demiflow.execution.contracts.dataflow_codec import encode_wire, runtime_scope_id
    from demiflow.execution.recovery_contract import document, identity
    store, bounds = execution._store, execution._bounds
    # This is the actual allocator-issued lease, not a synthetic worker ID.
    transport = _NativeTransport(execution._services)
    active = WorkerJournalClient(transport=transport, lease=assignment.lease, bounds=bounds)
    active.check_grant()
    assert len(transport.requests) == 1
    expired = replace(assignment.lease, expires_at_ms=int(time.time()*1000)-1)
    stale = WorkerJournalClient(transport=transport, lease=expired, bounds=bounds)
    result = {'expired_grant': _rejection(stale.check_grant,
        code='FENCED', text='grant has expired', allocation_id=expired.allocation_id)}
    assert len(transport.requests) == 1, 'expired grant reached the driver'
    pin_ref = assignment.sealed.code_pin_ref
    pin = json.loads(_read(store, pin_ref, bounds))
    pin['programs'] = {}
    wrong_pin = _put(store, document(pin), pin_ref.media_type)
    bad = replace(assignment, sealed=replace(assignment.sealed, code_pin_ref=wrong_pin))
    result['worker_code_pin'] = _rejection(lambda: _load_programs(bad, store=store, bounds=bounds),
        code='CODE_MISMATCH', text='Code-pin manifest membership differs', ref=asdict(wrong_pin))
    cross = replace(assignment, sealed=replace(assignment.sealed,
        code_pin_ref=replace(pin_ref, store_id='unrelated-domain')))
    result['worker_cross_domain'] = _rejection(lambda: make_runtime_services(cross,
        transport=transport, cache=root/'cross-domain-worker'),
        code='INVALID_CONTRACT', text='another store domain')
    assert _load_programs(assignment, store=store, bounds=bounds), 'real worker owns no verified programs'
    event = WorkerEvent(schema='demiflow.dataflow_event.v3', kind='progress',
        operation_id=identity('V033-oversized-control'), run_id=assignment.run_id,
        attempt=assignment.attempt, fence=assignment.fence,
        allocation_id=assignment.lease.allocation_id, boot_id=assignment.lease.boot_id,
        plan_revision=assignment.plan_revision,
        scope_id=runtime_scope_id(assignment.plan_id, assignment.attempt),
        controller_epoch=assignment.attempt, payload_ref=None,
        payload={'blob': 'x'*(bounds.max_control_bytes+1)})
    result['oversized_control'] = _rejection(lambda: encode_wire(event,
        type_name='WorkerEvent', bounds=bounds), code='BUDGET_EXCEEDED', text='budget',
        original_bound=bounds.max_control_bytes, attempted_payload_bytes=bounds.max_control_bytes+1)
    return result


def _bootstrap_attack(root):
    import os
    import subprocess
    import sys
    from demiflow.execution.worker.bootstrap import LAUNCH_ENV, CONTROL_FD_ENV, _MAX_LAUNCH_BYTES
    path = root/'oversized-launch.json'
    path.write_bytes(b' '*(_MAX_LAUNCH_BYTES+1))
    command = [sys.executable, '-B', '-m', 'demiflow.execution.worker.bootstrap']
    tree = str(Path(__file__).resolve().parents[1])
    result = subprocess.run(command, cwd=tree, env={**os.environ, 'PYTHONPATH':tree,
        LAUNCH_ENV: str(path), CONTROL_FD_ENV:'-1'}, capture_output=True, text=True, timeout=20)
    assert result.returncode != 0 and 'Worker launch spec exceeds byte budget' in result.stderr
    return {'status':'rejected', 'returncode':result.returncode, 'stderr':result.stderr,
            'command':command, 'original_bound':_MAX_LAUNCH_BYTES,
            'attempted_bytes':path.stat().st_size}


def validate_evidence(evidence):
    cases = evidence.get('counterexamples', {})
    assert set(cases) == REQUIRED_CASES, 'incomplete executed security counterexamples'
    assert all(c['status'] == 'rejected' for c in cases.values())
    for name, case in cases.items():
        if name == 'oversized_bootstrap':
            assert case['returncode'] != 0 and 'Worker launch spec exceeds byte budget' in case['stderr']
        else:
            assert case['exception'] == 'DataflowError' and case['code'] and case['message']
    for name in ('checksum', 'missing_historical_plan', 'missing_historical_program', 'missing_historical_source'):
        case = cases[name]
        assert case['original_sha256'] == case['restored_sha256'] == case['ref']['digest']
    assert evidence['positive']['output'] == [
        {'value':2,'payload_bytes':98304}, {'value':3,'payload_bytes':98304}]
    assert evidence['positive']['commits'] and all(c['status']=='acked' for c in evidence['positive']['commits'])
    assert evidence['launches'] and all(x['bytes'] < 65536 and x['reference_only'] for x in evidence['launches'])
    assert evidence['control_frames'] and all(x['count'] > 0 and x['maximum_bytes'] <= x['bound']
        and x['business_payload_matches'] == 0 for x in evidence['control_frames'].values())
    assert evidence['current_main_reads'] == []
    assert evidence['historical_recompiles'] == 0
    assert evidence['original_objects_restored'] is True


def artifact_security(fx, fault=None):
    from demiflow import data
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    from demiflow.execution.action_driver import dataflow as dataflow_module
    from demiflow.execution.hosts.endpoints import _EndpointBase, LocalHostEndpoint, RayHostEndpoint
    from demiflow.execution.contracts.dataflow_codec import encode_wire
    from demiflow.execution.worker.runtime import _decode_v3_assignment
    from demiflow.execution.recovery_contract import document
    from runtime_v3_residency_scenarios import _provider
    from runtime_v3_support import assert_tree_binding
    import builtins
    from demiflow.planning import dataflow_compile
    assert fault is None, 'V033 runs its complete finite fault matrix'
    assert_tree_binding()
    root = fx.root/'security'
    root.mkdir()
    cases, launches, executions, frames = {}, [], [], {}
    recompiles, main_reads = [], []
    main = Path('/yzp/zhaozy/yangzepeng/0905/demiflow')
    originals = DataflowExecution.prepare, DataflowExecution._assignment, _EndpointBase._build_launch
    original_lower = dataflow_compile.lower_action
    checking_history = False
    def lower(*args, **kwargs):
        if checking_history:
            recompiles.append(True)
        return original_lower(*args, **kwargs)
    def read_observer(original):
        def observe(path, *args, **kwargs):
            if isinstance(path, (str, bytes, Path)):
                p = Path(path.decode() if isinstance(path, bytes) else path)
                if p.is_absolute() and p.is_relative_to(main):
                    main_reads.append(str(p))
            return original(path, *args, **kwargs)
        return observe
    def prepare(execution):
        nonlocal checking_history
        sealed = originals[0](execution)
        if not cases:
            executions.append(execution)
            checking_history = True
            try:
                cases.update(_artifact_attacks(execution, root))
            finally:
                checking_history = False
        return sealed
    def assignment(execution, lease, stage):
        value = originals[1](execution, lease, stage)
        if 'expired_grant' not in cases:
            cases.update(_assignment_attacks(execution, value, root))
        return value
    def launch(endpoint, assigned):
        result = originals[2](endpoint, assigned)
        config = result[0]['dataflow_v3']
        assert set(config['assignment']) == {'ref'}
        decoded = _decode_v3_assignment({**config, 'transport':_NativeTransport(endpoint._services)})
        assert decoded == assigned
        raw = document(result[0])
        assert b'x'*1024 not in raw
        launches.append({'bytes':len(raw), 'reference_only':True,
            'assignment_ref':config['assignment']['ref'], 'allocation_id':assigned.lease.allocation_id})
        return result
    def frame(value, name, bounds):
        raw = encode_wire(value, type_name=name, bounds=bounds)
        assert b'x'*1024 not in raw, 'data payload travelled through the control channel'
        key = name + ':' + (value.kind if name == 'WorkerEvent' else value.status)
        facts = frames.setdefault(key, {'count':0,'maximum_bytes':0,
            'bound':bounds.max_control_bytes,'business_payload_matches':0})
        facts['count'] += 1
        facts['maximum_bytes'] = max(facts['maximum_bytes'], len(raw))
    endpoint_type = LocalHostEndpoint if fx.provider == 'local' else RayHostEndpoint
    original_poll, original_reply = endpoint_type.poll, endpoint_type.deliver_reply
    def poll(endpoint, **kwargs):
        values = original_poll(endpoint, **kwargs)
        for value in values:
            frame(value, 'WorkerEvent', endpoint._services.bounds)
        return values
    def reply(endpoint, value):
        frame(value, 'ControlReply', endpoint._services.bounds)
        return original_reply(endpoint, value)
    with _provider(fx, workers=2), patch.object(DataflowExecution, 'prepare', prepare), \
            patch.object(DataflowExecution, '_assignment', assignment), \
            patch.object(_EndpointBase, '_build_launch', launch), \
            patch.object(endpoint_type, 'poll', poll), patch.object(endpoint_type, 'deliver_reply', reply), \
            patch.object(dataflow_compile, 'lower_action', lower), \
            patch.object(dataflow_module, 'lower_action', lower), \
            patch.object(builtins, 'open', read_observer(builtins.open)), \
            patch.object(io, 'open', read_observer(io.open)):
        output = data.from_items([{'value':i,'payload':'x'*98304} for i in (1,2)]).map(_summarize).take_all()
    assert len(executions) == 1
    execution = executions[0]
    cases['oversized_bootstrap'] = _bootstrap_attack(root)
    commits = execution._action.context.journal.execution_commits(execution._action.action_id)
    evidence = {'counterexamples':cases, 'launches':launches, 'control_frames':frames,
        'positive':{'output':output, 'run_id':execution._run_id(),
            'action_id':execution._action.action_id, 'attempt':execution._attempt(),
            'dag_id':execution._graph.dag_id, 'plan_id':execution._physical.plan_id,
            'commits':[{'commit_id':c['commit_id'],'status':c['status']} for c in commits]},
        'current_main_reads':main_reads, 'historical_recompiles':len(recompiles),
        'original_objects_restored':all(c['original_sha256'] == c['restored_sha256']
            for c in cases.values() if 'original_sha256' in c),
        'scope':{'positive':'hosted public Dataset with original allocation and ACK',
                 'negatives':'real store/loader/bootstrap and allocator-issued grant; in-process transport only'}}
    validate_evidence(evidence)
    return evidence
