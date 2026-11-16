"""Fresh destination CAS precedes input; historical final ACKs remain readable."""
import lance
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.execution.run_context import RunContext
from demiflow.execution.dataflow import DataflowExecution
from demiflow.execution.dataflow_checkpoint import DataflowCheckpointCoordinator


def test_completed_overwrite_replay_uses_original_receipt_after_newer_head(tmp_path, monkeypatch):
    class Interrupted(Exception):
        pass
    uri = str(tmp_path/'table.lance')
    lance.write_dataset(pa.table({'value':[1]}), uri)
    original = DataflowCheckpointCoordinator.commit
    def stop_after_final_ack(coordinator, *args, **kwargs):
        image = original(coordinator, *args, **kwargs)
        if image.cut.final:
            raise Interrupted('final native write already ACKed')
        return image
    with pytest.raises(Interrupted, match='already ACKed'):
        with RunContext.start(tmp_path/'run') as run:
            ref = run.ref
            with monkeypatch.context() as patch:
                patch.setattr(DataflowCheckpointCoordinator, 'commit', stop_after_final_ack)
                data.from_items([{'value':2}]).write_lance(uri, mode='overwrite', expected_version=1,
                                                        return_receipt=True)
    assert lance.dataset(uri).version == 2
    before = run.journal.execution_commits('action-1')
    assert before[-1]['status'] == 'acked'
    lance.write_dataset(pa.table({'value':[3]}), uri, mode='append')
    def no_launch(*args, **kwargs):
        raise AssertionError('Final ACK replay must not restart input workers')
    with RunContext.resume(ref) as resumed:
        with monkeypatch.context() as patch:
            patch.setattr(DataflowExecution, '_prepare_launch', no_launch)
            receipt = data.from_items([{'value':2}]).write_lance(uri, mode='overwrite', expected_version=1,
                                                               return_receipt=True)
        assert receipt.status == 'committed' and receipt.committed_version == 2
        assert resumed.journal.execution_commits('action-1') == before
    assert lance.dataset(uri).version == 3
    assert lance.dataset(uri).to_table().to_pylist() == [{'value':2}, {'value':3}]
    assert lance.dataset(uri, version=1).to_table().to_pylist() == [{'value':1}]
