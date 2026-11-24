"""Publication root pins preserve native history and all descendant validation."""
from dataclasses import asdict
import json

import pytest

from demiflow.execution.checkpoint.commit import CommitCoordinator
from demiflow.execution.contracts.dataflow import ArtifactRef
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.storage.gc import collect_published_orphans
from demiflow.execution.storage.segments import SegmentForest, RECORDS_MEDIA, block_chunks, tree_refs
from demiflow.execution.outputs.sinks import _DataflowArtifactSink
from demiflow.execution.contracts.recovery_contract import SourcePosition
from test_runtime_v3_artifact_accounting import store_fixture


def _prefix(store, bounds, *, count=8):
    forest = SegmentForest(store, bounds)
    raw = b''
    for i in range(count):
        line = json.dumps({'value': i}).encode() + b'\n'
        ref = store.put(iter([line]), max_bytes=len(line), media_type=RECORDS_MEDIA)
        forest.append(ref, rows=1)
        raw += line
    return forest, raw


def _manifest(forest):
    return {'schema': 'demiflow.dataflow_sink_combined.v3', 'sink_id': 'test',
            'partitions': [{'partition_id': 'p0', 'rows': forest.rows,
                            'bytes': forest.bytes, 'data_ref': asdict(forest.seal())}],
            'total_rows': forest.rows, 'total_bytes': forest.bytes}


def _sink(store, port):
    return _DataflowArtifactSink(sink_id='test', store=store, action=port._action,
                                bounds=port._bounds, configuration={})


def _owners(run):
    with run.journal._connection() as db:
        return db.execute('SELECT digest,owner FROM artifact_owner ORDER BY digest,owner').fetchall()


def _gc(store, port):
    store.close()
    return collect_published_orphans(port._action, bounds=port._bounds)


@pytest.mark.parametrize('window', ['after_prepare_before_record', 'after_prepared', 'acked'])
def test_original_gc_traces_publication_root_before_record_and_through_commit(tmp_path, monkeypatch, window):
    with store_fixture(tmp_path) as (run, store, port):
        forest, expected = _prefix(store, port._bounds)
        manifest = _manifest(forest)
        root = ArtifactRef(**manifest['partitions'][0]['data_ref'])
        descendants = tuple(tree_refs(store, root, port._bounds))
        garbage = store.put(iter([b'not reachable']), max_bytes=20, media_type='application/octet-stream')
        sink = _sink(store, port)

        def interrupt(point, commit_id):
            if point == window:
                raise RuntimeError('publication reply lost')

        with monkeypatch.context() as patch:
            patch.setattr('demiflow.execution.checkpoint.commit.fault_point', interrupt)
            if window == 'acked':
                CommitCoordinator(port._action).commit(1, outputs={'test': (sink, manifest)},
                    position=SourcePosition('fixture', forest.rows))
            else:
                with pytest.raises(RuntimeError, match='reply lost'):
                    CommitCoordinator(port._action).commit(1, outputs={'test': (sink, manifest)},
                        position=SourcePosition('fixture', forest.rows))
        facts = run.journal.execution_commits(port._action.action_id)
        assert len(facts) == 1
        assert facts[0]['status'] == {'after_prepare_before_record': 'pending',
                                     'after_prepared': 'prepared', 'acked': 'acked'}[window]
        pins = _owners(run)
        assert len(pins) == 1 and pins[0][1] == facts[0]['commit_id']
        assert all(ref.digest != pins[0][0] for ref in descendants)
        if window == 'after_prepare_before_record':
            assert facts[0]['prepared'] is None  # Only the original owner root can protect these bytes.
        result = _gc(store, port)
        assert result == {'removed': 1, 'bytes': garbage.size_bytes, 'blocked': None}
        assert not store.inspect(garbage)
        assert all(store.inspect(ref) for ref in descendants)
        assert b''.join(block_chunks(store, root, port._bounds)) == expected
        assert run.journal.execution_commits(port._action.action_id) == facts


def test_each_historical_cut_keeps_one_root_and_original_bytes_after_gc(tmp_path):
    with store_fixture(tmp_path) as (run, store, port):
        forest, prefix = _prefix(store, port._bounds, count=4)
        sink = _sink(store, port)
        first = _manifest(forest)
        coordinator = CommitCoordinator(port._action)
        old = coordinator.commit(1, outputs={'test': (sink, first)}, position=SourcePosition('fixture', 4))
        suffix = b'{"value":4}\n'
        leaf = store.put(iter([suffix]), max_bytes=len(suffix), media_type=RECORDS_MEDIA)
        forest.append(leaf, rows=1)
        second = _manifest(forest)
        current = coordinator.commit(2, outputs={'test': (sink, second)}, position=SourcePosition('fixture', 5))
        assert len(_owners(run)) == 2
        assert old['status'] == current['status'] == 'acked'
        # Aborting a committed publication cannot release even its root pin.
        sink.abort(current['prepared']['sinks']['test'], coordinator._context(current['commit_id']))
        assert len(_owners(run)) == 2
        result = _gc(store, port)
        assert result['blocked'] is None
        for manifest, expected in ((first, prefix), (second, prefix + suffix)):
            root = ArtifactRef(**manifest['partitions'][0]['data_ref'])
            assert b''.join(block_chunks(store, root, port._bounds)) == expected
        assert run.journal.execution_commits(port._action.action_id) == [old, current]


@pytest.mark.parametrize('target,mutation', [('leaf', 'tamper'), ('index', 'tamper'),
                                              ('leaf', 'missing'), ('index', 'missing')])
def test_prepare_still_rejects_changed_or_missing_old_prefix(tmp_path, target, mutation):
    with store_fixture(tmp_path) as (run, store, port):
        forest, _ = _prefix(store, port._bounds)
        manifest = _manifest(forest)
        root = ArtifactRef(**manifest['partitions'][0]['data_ref'])
        refs = tuple(tree_refs(store, root, port._bounds))
        ref = next(ref for ref in refs if (ref.media_type == RECORDS_MEDIA) == (target == 'leaf'))
        path = store._object_path(ref)
        if mutation == 'missing':
            path.unlink()
        else:
            raw = path.read_bytes()
            path.write_bytes(bytes([raw[0] ^ 1]) + raw[1:])  # Same size; requires the original digest check.
        coordinator = CommitCoordinator(port._action)
        with pytest.raises((DataflowError, FileNotFoundError)):
            coordinator.commit(1, outputs={'test': (_sink(store, port), manifest)},
                               position=SourcePosition('fixture', forest.rows))
        facts = run.journal.execution_commits(port._action.action_id)
        assert len(facts) == 1 and facts[0]['status'] == 'pending'
        assert facts[0]['prepared'] is None and not facts[0]['receipts']
        assert _owners(run) == []
        store.close()


def test_abort_releases_legacy_descendant_pins_and_new_publication_pin(tmp_path):
    with store_fixture(tmp_path) as (run, store, port):
        forest, _ = _prefix(store, port._bounds)
        manifest = _manifest(forest)
        root = ArtifactRef(**manifest['partitions'][0]['data_ref'])
        refs = tuple(tree_refs(store, root, port._bounds))
        sink, coordinator = _sink(store, port), CommitCoordinator(port._action)
        context = coordinator._context('legacy-commit')
        # Reproduce the prior writer's actual owner pins in the original journal.
        for ref in refs:
            store.retain(ref, owner=context.commit_id)
        prepared = sink.prepare(manifest, context)
        assert len(_owners(run)) == len(refs) + 1
        sink.abort(prepared, context)
        sink.abort(prepared, context)
        assert _owners(run) == []
        result = _gc(store, port)
        assert result['blocked'] is None and result['removed'] >= len(refs) + 1
        assert not store.inspect(root)


def test_previously_checked_leaf_is_revalidated_after_same_size_edit(tmp_path):
    with store_fixture(tmp_path) as (run, store, port):
        forest, _ = _prefix(store, port._bounds, count=4)
        sink = _sink(store, port)
        manifest = _manifest(forest)
        coordinator = CommitCoordinator(port._action)
        prior = coordinator.commit(1, outputs={'test': (sink, manifest)}, position=SourcePosition('fixture', 4))
        root = ArtifactRef(**manifest['partitions'][0]['data_ref'])
        leaf = next(ref for ref in tree_refs(store, root, port._bounds) if ref.media_type == RECORDS_MEDIA)
        path = store._object_path(leaf)
        original = path.read_bytes()
        path.write_bytes(bytes([original[0] ^ 1]) + original[1:])
        suffix = b'{"value":4}\n'
        forest.append(store.put(iter([suffix]), max_bytes=len(suffix), media_type=RECORDS_MEDIA), rows=1)
        with pytest.raises(DataflowError, match='artifact bytes do not match'):
            coordinator.commit(2, outputs={'test': (sink, _manifest(forest))}, position=SourcePosition('fixture', 5))
        first, second = run.journal.execution_commits(port._action.action_id)
        assert first == prior and second['status'] == 'pending' and not second['receipts']
        store.close()
