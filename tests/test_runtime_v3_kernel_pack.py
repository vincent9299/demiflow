"""Packed physical carriers preserve original images, authority and recovery."""
from dataclasses import replace
import hashlib
import json
import sys

import pytest

from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.contracts.state_codec import PACK_MEDIA, _MAGIC, _header
from demiflow.execution.storage.streams import iter_kernel_images
from demiflow.execution.storage.streams import read_kernel_image
from demiflow.execution.kernels.image_pack import snapshot_kernel_images
from demiflow.execution.contracts import state_codec
from demiflow.execution.kernels.operator import OperatorKernel, IMAGE_MEDIA_TYPE
from test_runtime_v3_artifact_accounting import store_fixture, consumed
from test_runtime_v3_k import services, spec, program, InputPort, rec
from test_runtime_v3_published_gc import collect, put


def kernels(store, bounds, *, states=('none', 'none', 'none')):
    svc = services(store=store, bounds=bounds, programs={'fn': program(lambda row: {'v': row['v'] + 1})})
    result = {}
    for index, state in enumerate(states):
        kernel = OperatorKernel(spec('MapOp', {'program': 'fn', 'occurrence_id': 'occ-' + str(index)},
            inputs=[InputPort('input', 'incremental')], state=state), services=svc, partition_id='p0')
        kernel.start()
        for sequence in range(index + 1):
            list(kernel.push('input', rec({'v': sequence}, sequence=sequence)))
        result[(kernel._occurrence, 'p0')] = kernel
    return result


def test_original_images_restore_own_cursors_and_old_single_image_remains_readable(tmp_path):
    with store_fixture(tmp_path) as (_, store, port):
        original = kernels(store, port._bounds)
        expected = {key: kernel._snapshot_bytes() for key, kernel in original.items()}
        old = next(iter(original.values())).snapshot()
        packed = snapshot_kernel_images(original, store=store, bounds=port._bounds)
        assert len(set(packed.values())) == 1
        assert next(iter(packed.values())).media_type == PACK_MEDIA
        restored = kernels(store, port._bounds)
        for key, kernel in restored.items():
            raw = read_kernel_image(store, packed[key[0]], port._bounds, occurrence=key[0], partition=key[1])
            assert raw == expected[key]
            kernel.restore(packed[key[0]])
            assert kernel._emitted == original[key]._emitted
            # This pure kernel has no independent native-actor state path.
            assert getattr(kernel, 'instance', None) is None
        first = next(iter(restored.values()))
        first.restore(old)
        assert first._emitted == 1
        with pytest.raises(DataflowError, match='occurrence/partition'):
            read_kernel_image(store, packed[first._occurrence], port._bounds,
                              occurrence=first._occurrence, partition='different')


def test_mixed_stateful_images_and_small_workspace_keep_original_path(tmp_path):
    with store_fixture(tmp_path) as (_, store, port):
        original = kernels(store, port._bounds, states=('none', 'none', 'auto_json'))
        refs = snapshot_kernel_images(original, store=store, bounds=port._bounds)
        assert refs['occ-0'] == refs['occ-1']
        assert refs['occ-2'].media_type == IMAGE_MEDIA_TYPE
        for key, kernel in original.items():
            kernel.restore(refs[key[0]])
        small = replace(port._bounds, max_state_bytes=32768)
        refs = snapshot_kernel_images(original, store=store, bounds=small)
        assert all(ref.media_type == IMAGE_MEDIA_TYPE for ref in refs.values())


def test_large_global_budget_does_not_borrow_a_small_assigned_input_window(tmp_path):
    with store_fixture(tmp_path) as (_, store, port):
        original = kernels(store, port._bounds)
        small = replace(port._bounds, max_batch_bytes=16384)
        refs = snapshot_kernel_images(original, store=store, bounds=small)
        assert all(ref.media_type == IMAGE_MEDIA_TYPE for ref in refs.values())


@pytest.mark.parametrize('occupied', [0, 4096])
def test_header_overhead_cannot_expand_original_spill_quota(tmp_path, occupied):
    with store_fixture(tmp_path) as (_, store, port):
        original = kernels(store, port._bounds)
        required = sum(len(kernel._snapshot_bytes()) for kernel in original.values())
    with store_fixture(tmp_path / 'tight', ceiling=required + 64 + occupied) as (run, store, port):
        if occupied:
            put(store, b'x' * occupied)
        original = kernels(store, port._bounds)
        refs = snapshot_kernel_images(original, store=store, bounds=port._bounds)
        assert all(ref.media_type == IMAGE_MEDIA_TYPE for ref in refs.values())
        assert consumed(run) <= required + 64 + occupied


def test_unknown_packed_settlement_never_falls_back_or_refunds(tmp_path, monkeypatch):
    with store_fixture(tmp_path) as (run, store, port):
        original = kernels(store, port._bounds)
        artifact, pending = port.artifact, []
        def lose_reply(command):
            if command.kind == 'settle_staging':
                pending.append(command)
                raise RuntimeError('publication reply unknown')
            return artifact(command)
        with monkeypatch.context() as patch:
            patch.setattr(port, 'artifact', lose_reply)
            with pytest.raises(RuntimeError, match='reply unknown'):
                snapshot_kernel_images(original, store=store, bounds=port._bounds)
        assert len(pending) == 1 and pending[0].ref.media_type == PACK_MEDIA
        assert consumed(run) == pending[0].ref.size_bytes
        assert store.inspect(pending[0].ref)
        for _ in range(2):
            artifact(pending[0])
        assert consumed(run) == pending[0].ref.size_bytes


def test_changed_image_during_pack_is_rejected_without_publication(tmp_path, monkeypatch):
    with store_fixture(tmp_path) as (run, store, port):
        original = kernels(store, port._bounds)
        kernel = next(iter(original.values()))
        encode, called = kernel._snapshot_bytes, []
        def changed():
            called.append(True)
            return encode() + (b' ' if len(called) > 1 else b'')
        monkeypatch.setattr(kernel, '_snapshot_bytes', changed)
        with pytest.raises(DataflowError, match='changed during checkpoint'):
            snapshot_kernel_images(original, store=store, bounds=port._bounds)
        assert consumed(run) == 0
        assert not list((store.root / store.store_id / 'objects').iterdir())


def test_header_cannot_route_another_original_image_to_requested_occurrence(tmp_path):
    with store_fixture(tmp_path) as (_, store, port):
        originals = kernels(store, port._bounds)
        raw = next(iter(originals.values()))._snapshot_bytes()
        ref = carrier(store, [('wrong', 'p0', raw), ('other', 'p0', raw)])
        with pytest.raises(DataflowError, match='original image identity'):
            read_kernel_image(store, ref, port._bounds, occurrence='wrong', partition='p0')


def carrier(store, images):
    raw = _MAGIC + str(len(images)).encode() + b'\n'
    for occurrence, partition, image in images:
        raw += _header(occurrence, partition, image)[1] + image + b'\n'
    return put(store, raw, PACK_MEDIA)


@pytest.mark.parametrize('fault', ['digest', 'duplicate', 'truncate', 'tail', 'version'])
def test_bad_late_member_rejects_even_when_requested_first_member_is_valid(tmp_path, fault):
    with store_fixture(tmp_path) as (_, store, port):
        images = [('a', 'p0', b'first'), ('b', 'p0', b'second')]
        good = carrier(store, images)
        raw = b''.join(store.read(good, max_bytes=good.size_bytes))
        if fault == 'digest':
            raw = raw.replace(b'second', b'seconX')
        elif fault == 'duplicate':
            raw = raw.replace(b'["b","p0"', b'["a","p0"')
        elif fault == 'truncate':
            raw = raw[:-2]
        elif fault == 'tail':
            raw += b'x'
        else:
            raw = raw.replace(b'pack.v1', b'pack.v9')
        bad = put(store, raw, PACK_MEDIA)
        with pytest.raises(DataflowError):
            read_kernel_image(store, bad, port._bounds, occurrence='a', partition='p0')


def test_gc_traces_all_nested_payload_refs_in_pack_and_preserves_history(tmp_path):
    with store_fixture(tmp_path) as (_, store, port):
        refs = [put(store, value) for value in (b'actor', b'pending', b'buffer')]
        images = [(str(i), 'p0', state_codec.dumps({name: ref}, max_bytes=8192))
                  for i, (name, ref) in enumerate(zip(('snapshot', 'pending', 'buffer'), refs))]
        packed = carrier(store, images)
        store.retain(packed, owner='committed-history')
        garbage = put(store, b'unreachable')
        result = collect(store, port)
        assert result == {'removed': 1, 'bytes': garbage.size_bytes, 'blocked': None}
        assert all(store.inspect(ref) for ref in (*refs, packed))


def test_gc_never_collects_after_partial_pack_trace(tmp_path):
    with store_fixture(tmp_path) as (_, store, port):
        child = put(store, b'protected')
        good = carrier(store, [('a', 'p0', state_codec.dumps({'ref': child}, max_bytes=8192)),
                               ('b', 'p0', b'invalid-json')])
        store.retain(good, owner='committed-history')
        garbage = put(store, b'unreachable')
        result = collect(store, port)
        assert result['blocked'] and result['removed'] == 0
        assert store.inspect(child) and store.inspect(garbage)


def test_store_releases_previous_chunk_before_next_image_is_constructed(tmp_path):
    with store_fixture(tmp_path) as (_, store, _):
        def chunks():
            raw = b'a' * 40000
            yield raw
            assert sys.getrefcount(raw) == 2  # This generator and getrefcount only.
            del raw
            yield b'b' * 40000
        ref = store.put(chunks(), max_bytes=80000, media_type='application/octet-stream')
        assert ref.size_bytes == 80000


def _increment(row):
    return {'v': row['v'] + 1}


def _double(row):
    return {'v': row['v'] * 2}


@pytest.mark.parametrize('old_carrier', [False, True])
def test_real_dataset_restores_committed_pack_or_old_images_then_continues_packing(tmp_path, monkeypatch, old_carrier):
    from demiflow import data
    from demiflow.execution.run_context import RunContext
    from demiflow.execution.checkpoint.coordinator import DataflowCheckpointCoordinator
    from demiflow.execution.contracts.dataflow_codec import compute_participant_id
    class Interrupted(Exception):
        pass
    def flow():
        return data.from_items([{'v': i} for i in range(12)]).map(_increment).map(_double)
    original_commit = DataflowCheckpointCoordinator.commit
    committed = []
    def stop(coordinator, *args, **kwargs):
        # Emulate old physical storage BEFORE the original commit/ACK; no old
        # history is rewritten and no coordinator step is bypassed.
        if old_carrier:
            pending = coordinator._pending[args[0]]
            partitions = {compute_participant_id(s.stage_id, p): p
                          for s in coordinator._physical.stages for p in s.partitions}
            for participant, contribution in list(pending.contributions.items()):
                states = dict(contribution.operator_states)
                for occurrence, ref in states.items():
                    if ref.media_type == PACK_MEDIA:
                        raw = read_kernel_image(coordinator._store, ref, coordinator._physical.bounds,
                            occurrence=occurrence, partition=partitions[participant])
                        states[occurrence] = coordinator._store.put(iter((raw,)), max_bytes=len(raw),
                                                                  media_type=IMAGE_MEDIA_TYPE)
                pending.contributions[participant] = replace(contribution, operator_states=states)
                # Digest is the original bounded contribution calculation.
                from demiflow.execution.contracts.checkpoint import contribution_digest as _contribution_digest
                pending.digests[participant] = _contribution_digest(pending.contributions[participant])
        image = original_commit(coordinator, *args, **kwargs)
        assert not image.cut.final
        refs = [ref for c in image.contributions for ref in c.operator_states.values()]
        assert any(ref.media_type == (IMAGE_MEDIA_TYPE if old_carrier else PACK_MEDIA) for ref in refs)
        committed.append(image.commit_id)
        raise Interrupted('original cut is ACKed')
    limits = {'checkpoint_rows': 2, 'checkpoint_interval_ms': 60000,
              'memory_bytes': 256 * 1024**2, 'max_state_bytes': 2 * 1024**2}
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path / 'run', limits=limits) as run, data.local_execution(workers=2):
            reference = run.ref
            with monkeypatch.context() as patch:
                patch.setattr(DataflowCheckpointCoordinator, 'commit', stop)
                flow().take_all()
    with RunContext.resume(reference) as run, data.local_execution(workers=2):
        before = run.journal.execution_commits('action-1')
        assert flow().take_all() == [{'v': (i + 1) * 2} for i in range(12)]
        after = run.journal.execution_commits('action-1')
        assert after[:len(before)] == before
        assert committed[0] in {item['commit_id'] for item in after}
        assert all(item['status'] == 'acked' for item in after)
        with run.journal._connection() as connection:
            assert connection.execute("SELECT count(*) FROM runtime_dataflow_staging WHERE publication LIKE '%kernel-pack-v1%'").fetchone()[0] > 1
