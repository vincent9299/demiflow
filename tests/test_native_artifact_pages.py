"""Paged history validation remains bounded and detects damaged references."""
import json

import pytest

from demiflow.execution.artifact_manifest import ArtifactManifest, manifest_pages
from demiflow.execution.recovery_contract import RecoveryContractError
from demiflow.execution.run_context import RunContext


@pytest.mark.parametrize('damage', ['missing', 'truncated', 'reordered', 'changed', 'index'])
def test_authenticated_pages_reject_damage(tmp_path, damage):
    with RunContext.start(tmp_path/'run') as run:
        manifest = ArtifactManifest(run, run.directory/'materializations/pages-test')
        for number in range(3):
            manifest.append([{'id': number}])
        reference = manifest.seal()
        assert [row for _, page in manifest_pages(run.directory, reference) for row in page] == [
            {'id': i} for i in range(3)]
        first, second = manifest.root/'0.json', manifest.root/'1.json'
        if damage == 'missing':
            second.unlink()
        elif damage == 'truncated':
            second.write_bytes(second.read_bytes()[:2])
        elif damage == 'reordered':
            a, b = first.read_bytes(), second.read_bytes()
            first.write_bytes(b)
            second.write_bytes(a)
        elif damage == 'changed':
            second.write_text('[{"id":9}]')
        else:
            (manifest.root/'index.json').write_text('{}')
        with pytest.raises((RecoveryContractError, FileNotFoundError, json.JSONDecodeError)):
            list(manifest_pages(run.directory, reference))


def test_state_rejects_escape_expansion_before_full_encoding(tmp_path, monkeypatch):
    import asyncio
    import demiflow.execution.operator_state as state
    class EscapedState:
        def snapshot_state(self):
            return {'text': '\x00' * 20000}

        def restore_state(self, value):
            pass
    calls = []
    monkeypatch.setattr(state, 'document', lambda *args: calls.append(args))
    with RunContext.start(tmp_path/'run', limits={'max_state_bytes': 65536}) as run:
        with pytest.raises(RecoveryContractError, match='encoding.*byte budget'):
            asyncio.run(state.OperatorStates(run).register(EscapedState(), 'node-0'))
    assert calls == []


def test_partial_reference_binding_is_unprotected_and_cleanup_reclaims_it(tmp_path):
    from demiflow.execution.recovery_contract import SourcePosition
    with pytest.raises(RecoveryContractError, match='unconfirmed actions'), RunContext.start(tmp_path/'run') as run:
        with run.action({'kind': 'materialize', 'source': {'fixture': 'partial-page'}}) as action:
            fact = run.journal.begin_commit(run.fence, action.action_id, 1,
                {'position': SourcePosition('source', 0).to_dict(), 'sinks': ['output'],
                 'states': [], 'requires_ack': False})
            manifest = ArtifactManifest(run, run.directory/'materializations/partial-page')
            for page in range(2):
                values = []
                for number in range(64):
                    path = run.directory/'scratch'/f'{page}-{number}'
                    run.allocate_artifact(path, atomic=False)
                    path.parent.mkdir(exist_ok=True)
                    path.write_bytes(b'x')
                    values.append({'path': str(path.relative_to(run.directory))})
                manifest.append(values)
            reference = manifest.seal()
            (manifest.root/'1.json').unlink()
            with pytest.raises(FileNotFoundError):
                run.journal.prepare_commit(run.fence, fact['commit_id'], {'sinks': reference})
            with run.journal._connection() as db:
                assert db.execute('SELECT count(*) FROM runtime_artifacts WHERE reference_commit_id=?',
                                  (fact['commit_id'],)).fetchone()[0] >= 64
                assert db.execute('SELECT count(*) FROM runtime_artifacts WHERE protected=1').fetchone()[0] == 0
            assert run.journal.execution_commit(fact['commit_id'])['status'] == 'pending'
            assert run.journal.clean_orphans(run.fence) >= 128
            assert not (run.directory/'scratch/0-0').exists()


def test_lazy_materialized_history_verifies_earlier_object_bytes(tmp_path):
    from types import SimpleNamespace
    from demiflow.execution.commit import CommitCoordinator
    from demiflow.execution.executors.local import _CheckpointBlocks
    from demiflow.execution.object_sink import ObjectRowsSink
    from demiflow.execution.recovery_contract import SourcePosition
    with RunContext.start(tmp_path/'run') as run:
        with run.action({'kind': 'materialize', 'source': {'fixture': 'object-history'}}) as action:
            sink = ObjectRowsSink(action, max_object_bytes=64)
            coordinator = CommitCoordinator(action)
            paths = []
            for index, raw in enumerate((b'first', b'later'), 1):
                ref = sink.objects.stage(raw)
                paths.append(sink.objects.root/ref['sha256'][:2]/ref['sha256'])
                coordinator.commit(index, outputs={'output': (sink, iter([{'image': ref}]))},
                    position=SourcePosition('object-history', index))
                sink.reset()
            blocks = _CheckpointBlocks(SimpleNamespace(run=run, action=action,
                sequence=2, row_sink=sink.rows, has_objects=True, sink=sink, coordinator=coordinator))
            action.complete({'rows': 2})
    assert len(list(blocks)) == 2
    paths[0].write_bytes(b'xxxxx')
    with pytest.raises(RecoveryContractError, match='checksum'):
        list(blocks)
