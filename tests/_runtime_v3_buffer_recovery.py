"""Read only the original acknowledged v3 cut and its bounded operator images."""
import contextlib

from demiflow.execution import commit as protocol
from demiflow.execution.contracts.dataflow import ArtifactRef
from demiflow.execution.contracts import state_codec
from demiflow.execution.kernels.operator import IMAGE_MEDIA_TYPE
from demiflow.execution.run_journal import RunJournal
from demiflow.execution.storage.state_artifacts import StateArtifacts
from demiflow.execution.seal.sealed_plans import _v3_read_store, load_sealed_plan


def images_at(run, fact, kind):
    image = StateArtifacts(run).read(fact['prepared']['state']['dataflow-cut-v3'])
    assert image['schema'] == 'demiflow.dataflow_cut.v3'
    result = []
    assert len(image['contributions']) <= 128
    for contribution in image['contributions']:
        assert len(contribution['operator_states']) <= 128
        for raw_ref in contribution['operator_states'].values():
            ref = ArtifactRef(**raw_ref)
            if ref.media_type != IMAGE_MEDIA_TYPE:
                # Other refs are finite-port indices and worker frontiers.
                continue
            store, _ = _v3_read_store(run.directory, store_id=ref.store_id)
            try:
                raw = b''.join(store.read(ref, max_bytes=run.limits['max_state_bytes']))
                snapshot = state_codec.loads(raw, max_bytes=run.limits['max_state_bytes'])
            finally:
                store.close()
            if snapshot.get('kind') == kind:
                result.append(snapshot)
    return image, result


@contextlib.contextmanager
def crash_with_remainder(monkeypatch, run_holder, *, kind, pending):
    fired = []
    def stop(name, commit_id):
        if name != 'after_acked' or fired:
            return
        run = run_holder[0]
        fact = run.journal.execution_commit(commit_id)
        if fact['action_id'] != 'action-1' or 'dataflow-cut-v3' not in fact['prepared']['state']:
            return
        cut, snapshots = images_at(run, fact, kind)
        if not cut['cut']['final'] and any(pending(item) for item in snapshots):
            fired.append(fact)
            raise RuntimeError('crash after publication')
    with monkeypatch.context() as patch:
        patch.setattr(protocol, 'fault_point', stop)
        yield fired
    assert fired, 'no acknowledged cut with the required genuine remainder was reached'


def assert_prefix_unchanged(before, after):
    acked = {fact['commit_id']: fact for fact in after if fact['status'] == 'acked'}
    assert all(fact['commit_id'] in acked for fact in before)
    for fact in before:
        assert acked[fact['commit_id']]['receipts'] == fact['receipts']
        assert acked[fact['commit_id']]['prepared'] == fact['prepared']
    assert [fact['sequence'] for fact in after] == list(range(1, len(after) + 1))
