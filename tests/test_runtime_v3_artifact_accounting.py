"""Artifact byte admission uses actual objects and one native budget authority."""
from contextlib import contextmanager
from dataclasses import replace
import hashlib

import pytest

from demiflow import data
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.execution.executors.dataflow_api import make_action_request
from demiflow.execution.storage.staging import DriverArtifactPort
from demiflow.execution.storage.objects import RunArtifactStore
from demiflow.execution.recovery_contract import RecoveryContractError
from demiflow.execution.recovery_contract import identity
from demiflow.execution.contracts.dataflow import ArtifactCommand
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.planning.dataflow_policy import resolve_runtime_policy
from test_runtime_v3_real_seams import PreparationOnly


@contextmanager
def store_fixture(tmp_path, *, ceiling=4 * 1024**2):
    with pytest.raises(PreparationOnly):
        with RunContext.start(tmp_path / 'run') as run:
            dataset = data.from_items([])
            request = make_action_request(dataset._executor, dataset._source, dataset._plan,
                                          kind='take', arguments={'limit': 1})
            policy, bounds = resolve_runtime_policy({}, run.limits, provider='local')
            bounds = replace(bounds, max_spill_bytes=ceiling)
            port = DriverArtifactPort(request.action, bounds=bounds)
            store = RunArtifactStore(root=run.directory / '.dataflow-store', store_id=policy.store_id,
                                     journal=port, bounds=bounds)
            yield run, store, port
            raise PreparationOnly()


def consumed(run):
    with run.journal._connection() as db:
        return db.execute("SELECT consumed FROM runtime_budgets WHERE name='artifact_staging_bytes'").fetchone()[0]


def test_repeated_small_object_only_charges_unique_actual_bytes(tmp_path):
    with store_fixture(tmp_path, ceiling=2048) as (run, store, port):
        for _ in range(30):
            ref = store.put(iter([b'one immutable object']), max_bytes=1024, media_type='application/octet-stream')
        assert consumed(run) == len(b'one immutable object')
        assert store.inspect(ref)
        assert not list((store.root / store.store_id / 'staging').iterdir())


def test_failed_iterator_and_creation_release_only_owned_staging(tmp_path, monkeypatch):
    with store_fixture(tmp_path, ceiling=2048) as (run, store, port):
        def broken():
            yield b'partial'
            raise RuntimeError('upstream encoding failed')
        for _ in range(4):
            with pytest.raises(RuntimeError, match='upstream'):
                store.put(broken(), max_bytes=1024, media_type='application/octet-stream')
        assert consumed(run) == 0
        import demiflow.execution.storage.objects as module
        with monkeypatch.context() as patch:
            def full(*args, **kwargs):
                raise OSError('disk full')
            patch.setattr(module, '_create_staging', full)
            with pytest.raises(OSError, match='disk full'):
                store.put(iter([b'x']), max_bytes=1024, media_type='application/octet-stream')
        assert consumed(run) == 0
        with pytest.raises(DataflowError, match='max_bytes'):
            store.put(iter([b'x' * 1025]), max_bytes=1024, media_type='application/octet-stream')
        assert consumed(run) == 0


def test_settlement_is_idempotent_and_unknown_publication_keeps_budget(tmp_path, monkeypatch):
    with store_fixture(tmp_path, ceiling=2048) as (run, store, port):
        artifact = port.artifact
        pending = []
        def lose_reply(command):
            if command.kind == 'settle_staging':
                pending.append(command)
                raise RuntimeError('authority response unknown')
            return artifact(command)
        with monkeypatch.context() as patch:
            patch.setattr(port, 'artifact', lose_reply)
            with pytest.raises(RuntimeError, match='response unknown'):
                store.put(iter([b'published']), max_bytes=1024, media_type='application/octet-stream')
        assert consumed(run) == 1024
        assert len(pending) == 1 and store.inspect(pending[0].ref)
        for _ in range(2):
            assert port.artifact(pending[0]).charged_bytes == len(b'published')
        assert consumed(run) == len(b'published')
        with pytest.raises(RecoveryContractError, match='settlement changed'):
            port.artifact(replace(pending[0], kind='release_staging', ref=None, size_bytes=0))
        assert consumed(run) == len(b'published')


def test_large_declared_chunk_streams_without_extra_whole_buffer_copy(tmp_path):
    with store_fixture(tmp_path) as (run, store, port):
        payload = b'x' * (1024**2 + 7)
        ref = store.put(iter([payload]), max_bytes=2 * 1024**2, media_type='application/octet-stream')
        assert ref.digest == hashlib.sha256(payload).hexdigest()
        assert b''.join(store.read(ref, max_bytes=len(payload))) == payload
        assert consumed(run) == len(payload)


def test_release_cannot_refund_a_live_owned_temporary(tmp_path):
    with store_fixture(tmp_path, ceiling=2048) as (run, store, port):
        owner = identity('owned-temporary')
        port.reserve(owner, {'artifact_staging_bytes': 1024})
        pending = store.root / store.store_id / 'staging' / ('.pending-' + owner + '-test')
        pending.write_bytes(b'partial')
        command = ArtifactCommand(identity([owner, 'release']), 'release_staging', None, owner, 0)
        with pytest.raises(DataflowError, match='deletion'):
            port.artifact(command)
        assert consumed(run) == 1024
        pending.unlink()
        port.artifact(command)
        assert consumed(run) == 0


def test_stream_growth_is_idempotent_and_quota_precedes_write(tmp_path, monkeypatch):
    with store_fixture(tmp_path, ceiling=256 * 1024) as (run, store, port):
        apply = port.artifact
        grows = []
        def inspect_growth(command):
            if command.kind == 'grow_staging':
                owned = list((store.root / store.store_id / 'staging').glob('.pending-' + command.owner + '-*'))
                assert len(owned) == 1
                assert owned[0].stat().st_size < command.size_bytes
                first = apply(command)
                before = consumed(run)
                assert apply(command) == first
                assert consumed(run) == before
                grows.append(command)
                return first
            return apply(command)
        monkeypatch.setattr(port, 'artifact', inspect_growth)
        ref = store.put(iter([b'a' * 32768, b'b' * 65536]), max_bytes=256 * 1024,
                        media_type='application/octet-stream')
        assert consumed(run) == ref.size_bytes == 98304
        assert grows
        with pytest.raises(RecoveryContractError, match='Settled staging'):
            apply(grows[0])
        with pytest.raises(RecoveryContractError, match='budget exhausted'):
            store.put(iter([b'x' * 200000]), max_bytes=256 * 1024,
                      media_type='application/octet-stream')
        assert consumed(run) == 98304
        assert not list((store.root / store.store_id / 'staging').iterdir())
        assert len(list((store.root / store.store_id / 'objects').iterdir())) == 1
