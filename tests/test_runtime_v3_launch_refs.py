"""Original bounded store/GC owns thin worker launch references."""
from dataclasses import asdict, replace
from types import SimpleNamespace as NS
import json

import pytest

from demiflow import data
from demiflow.execution.contracts.dataflow import CutRequest, RecoveryImage, SourceCursor
from demiflow.execution.contracts.dataflow_codec import encode_wire
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.dataflow_gc import collect_published_orphans
from demiflow.execution.dataflow_store import RunArtifactStore
from demiflow.execution.recovery_contract import identity
from demiflow.execution.run_context import RunContext
from demiflow.execution.worker.runtime import _decode_v3_assignment, _decode_v3_ref, WorkerRuntime
from test_runtime_v3_artifact_accounting import store_fixture

MEDIA = 'application/x-demiflow-recovery-image-v3'


def _image(payload=None):
    cursor = SourceCursor(identity('split'), {'row': 0, 'opaque': 'x' * 70000}, 0,
                          False, None, (payload,) if payload else ())
    cut = CutRequest(identity('cut'), 1, 0, 0, ('participant',), False)
    return RecoveryImage(identity('commit'), cut, (), {}, (cursor,))


def test_large_typed_image_keeps_original_gc_reference_graph(tmp_path):
    with store_fixture(tmp_path) as (run, store, port):
        bounds = port._bounds
        payload = store.put(iter([b'original-payload']), max_bytes=128, media_type='text/plain')
        image = _image(payload)
        raw = encode_wire(image, type_name='RecoveryImage', bounds=bounds)
        assert len(raw) > 65536
        ref = store.put(iter([raw]), max_bytes=bounds.max_control_bytes, media_type=MEDIA)
        assert _decode_v3_ref(asdict(ref), type_name='RecoveryImage', media_type=MEDIA,
                              store=store, bounds=bounds) == image
        store.retain(ref, owner='original-launch-proof')
        garbage = store.put(iter([b'unreferenced']), max_bytes=128, media_type='text/plain')
        assert collect_published_orphans(port._action, bounds=bounds)['blocked']
        store.close()
        result = collect_published_orphans(port._action, bounds=bounds)
        assert result['blocked'] is None and result['bytes'] == garbage.size_bytes
        assert store.inspect(ref) and store.inspect(payload) and not store.inspect(garbage)
        reopened = RunArtifactStore(root=store.root, store_id=store.store_id, journal=port, bounds=bounds)
        try:
            reopened.release(ref, owner='original-launch-proof')
        finally:
            reopened.close()
        result = collect_published_orphans(port._action, bounds=bounds)
        assert result['blocked'] is None and result['bytes'] == ref.size_bytes + payload.size_bytes


def test_launch_ref_rejects_wrong_media_store_budget_and_replaced_bytes(tmp_path):
    with store_fixture(tmp_path) as (_, store, port):
        bounds = port._bounds
        raw = encode_wire(_image(), type_name='RecoveryImage', bounds=bounds)
        ref = store.put(iter([raw]), max_bytes=bounds.max_control_bytes, media_type=MEDIA)
        value = asdict(ref)
        def decode(value, ceiling=bounds):
            return _decode_v3_ref(value, type_name='RecoveryImage', media_type=MEDIA,
                                   store=store, bounds=ceiling)
        with pytest.raises(DataflowError, match='type differs'):
            decode({**value, 'media_type': 'application/json'})
        with pytest.raises(DataflowError, match='store'):
            decode({**value, 'store_id': 'wrong-store'})
        with pytest.raises(DataflowError, match='control budget'):
            decode(value, replace(bounds, max_control_bytes=65536))
        with pytest.raises(DataflowError):
            decode({**value, 'key': '../outside'})
        path = store.root / store.store_id / 'objects' / ref.key
        path.write_bytes(raw.replace(b'xxxxx', b'yyyyy', 1))
        with pytest.raises(DataflowError, match='bytes do not match'):
            decode(value)
        store.close()


def test_assignment_and_recovery_ambiguous_carriers_fail_before_executor(tmp_path):
    with store_fixture(tmp_path) as (_, store, port):
        with pytest.raises(DataflowError, match='exactly one'):
            _decode_v3_assignment({'assignment': {'wire': '{}', 'ref': {}}, 'bounds': port._bounds})
        store.close()
    runtime = object.__new__(WorkerRuntime)
    runtime._v3 = True
    runtime._v3_config = {'recovery_image_wire': '{}', 'recovery_image_ref': None}
    runtime._v3_ensure_executor = lambda: pytest.fail('ambiguous restore constructed executor')
    with pytest.raises(DataflowError, match='exactly one'):
        runtime._restore([])


def test_original_inline_recovery_wire_remains_readable(tmp_path):
    with store_fixture(tmp_path) as (_, store, port):
        runtime = object.__new__(WorkerRuntime)
        runtime._v3 = True
        image = _image()
        runtime._v3_config = {'recovery_image_wire': encode_wire(image,
            type_name='RecoveryImage', bounds=port._bounds).decode()}
        restored = []
        executor = NS(_services=NS(store=store, bounds=port._bounds), restore=restored.append)
        runtime._v3_ensure_executor = lambda: executor
        assert runtime._restore([]) == [] and restored == [image]
        store.close()


def test_explicit_null_recovery_reference_cannot_become_fresh_execution(tmp_path):
    with store_fixture(tmp_path) as (_, store, port):
        runtime = object.__new__(WorkerRuntime)
        runtime._v3 = True
        runtime._v3_config = {'recovery_image_ref': None}
        executor = NS(_services=NS(store=store, bounds=port._bounds),
                      restore=lambda _: pytest.fail('invalid history became fresh execution'))
        runtime._v3_ensure_executor = lambda: executor
        with pytest.raises(DataflowError, match='invalid worker launch artifact reference'):
            runtime._restore([])
        store.close()


def test_real_process_assignment_and_restore_launch_stay_below_original_64k(tmp_path, monkeypatch):
    from demiflow.execution.dataflow_hosts import _EndpointBase
    from demiflow.execution.worker.bootstrap import _MAX_LAUNCH_BYTES
    from demiflow.execution.dataflow_checkpoint import DataflowCheckpointCoordinator
    observed, restores = [], []
    build, commit = _EndpointBase._build_launch, DataflowCheckpointCoordinator.commit
    def inspect(endpoint, assignment):
        result = build(endpoint, assignment)
        launch = result[0]
        config = launch['dataflow_v3']
        assert set(config['assignment']) == {'ref'}
        assert config['assignment']['ref']['media_type'] == 'application/x-demiflow-worker-assignment-v3'
        size = len(json.dumps(launch).encode())
        assert size < _MAX_LAUNCH_BYTES == 65536
        assert _decode_v3_assignment({'assignment': {'wire': encode_wire(assignment,
            type_name='WorkerAssignment', bounds=assignment.bounds).decode()},
            'bounds': assignment.bounds}) == assignment
        observed.append(size)
        if 'recovery_image_ref' in config:
            assert 'recovery_image_wire' not in config
            restores.append(config['recovery_image_ref'])
        return result
    monkeypatch.setattr(_EndpointBase, '_build_launch', inspect)
    class Interrupted(Exception): pass
    def interrupt(coordinator, *args, **kwargs):
        image = commit(coordinator, *args, **kwargs)
        if not image.cut.final:
            raise Interrupted()
        return image
    def pipeline():
        return data.from_items([{'i': i} for i in range(6)]).map(lambda row: row)
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path/'run', limits={'checkpoint_rows':2,
                'checkpoint_interval_ms':60000}) as run, data.local_execution(workers=2, worker_mode='process'):
            reference = run.ref
            with monkeypatch.context() as patch:
                patch.setattr(DataflowCheckpointCoordinator, 'commit', interrupt)
                pipeline().take_all()
    with RunContext.resume(reference) as run, data.local_execution(workers=2, worker_mode='process'):
        assert pipeline().take_all() == [{'i': i} for i in range(6)]
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
    assert observed and restores


def test_actual_bootstrap_still_rejects_oversized_launch_before_control(tmp_path):
    import os
    import subprocess
    import sys
    from demiflow.execution.worker.bootstrap import LAUNCH_ENV, CONTROL_FD_ENV
    path = tmp_path/'oversized-launch.json'
    path.write_bytes(b' ' * 65537)
    result = subprocess.run([sys.executable, '-B', '-m', 'demiflow.execution.worker.bootstrap'],
        env={**os.environ, LAUNCH_ENV: str(path), CONTROL_FD_ENV: '-1'},
        capture_output=True, text=True, timeout=15)
    assert result.returncode != 0 and 'Worker launch spec exceeds byte budget' in result.stderr
