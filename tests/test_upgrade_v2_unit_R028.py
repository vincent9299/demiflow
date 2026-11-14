"""R028-R032 · graph worker cut identity handshake (verify_preserve evidence).

The driver names graph cuts; the worker only records the named bound/finality,
aligns its input ordinal, and releases post-cut data once the driver's single
durable commit acks. These methods are baseline-preserved; this file pins their
observable contract directly without a Ray graph allocation.
"""
import threading
from types import SimpleNamespace

import pytest

from demiflow.execution.run_context import RunContext
from demiflow.execution.worker.graph_progress import GraphWorkerProgress


@pytest.fixture
def progress(tmp_path):
    with RunContext.start(tmp_path / 'run') as run:
        with run.action({'kind': 'materialize', 'source': {'fixture': 'graph-cut'}}) as action:
            gp = GraphWorkerProgress(action, allocation_id='alloc-1', occurrence_id='occ-1',
                                     source_id='src-1', logical_node_id='node-0')
            yield gp
            action.complete({'kind': 'materialized', 'rows': 0})


def test_request_cut_records_named_bound_epoch_and_finality(progress):
    progress.request_cut('ck-1', 7, epoch=3, final=True)
    assert progress._cut_checkpoint_id == 'ck-1'
    assert progress.pending_cut == 7
    assert progress._cut_epoch == 3
    assert progress._final_cut is True


def test_note_driver_cut_finality_drives_align_cut(progress):
    progress.note_driver_cut('ck-2', final=True)
    assert progress._driver_finals['ck-2'] is True
    done = progress.align_cut('ck-2', 5)
    assert isinstance(done, threading.Event) and not done.is_set()
    assert progress.pending_cut == 5
    assert progress._final_cut is True  # taken from the driver hint, not the arg


def test_align_cut_defaults_final_false_without_driver_hint(progress):
    progress.align_cut('ck-3', 2)
    assert progress._final_cut is False


def test_note_contribution_acked_releases_reader_and_advances_inputs(progress):
    done = progress.align_cut('ck-4', 4)
    assert not done.is_set()
    progress.note_contribution_acked(4)
    assert done.is_set()
    assert progress.inputs == 4


def test_note_contribution_acked_without_pending_event_is_safe(progress):
    progress.note_contribution_acked(9)
    assert progress.inputs == 9


def test_channel_cuts_report_input_and_output_bounds(progress):
    progress.input_reader = SimpleNamespace(
        reader_id='reader-1', cursors={'chan-in': 4})
    producer = SimpleNamespace(next_sequence=6)
    progress.output_router = SimpleNamespace(
        channels={'chan-out': SimpleNamespace(producer=producer)})
    cuts = {c['channel_id']: c for c in progress._channel_cuts()}
    assert cuts['chan-in']['producer_cut_sequence'] == 5
    assert cuts['chan-in']['consumer_offsets'] == {'reader-1': 5}
    assert cuts['chan-out']['producer_cut_sequence'] == 5
    assert cuts['chan-out']['consumer_offsets'] == {}


def test_channel_cuts_empty_without_reader_or_router(progress):
    assert progress._channel_cuts() == []